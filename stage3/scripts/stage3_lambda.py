import json
import logging
import os
import time
from datetime import datetime
from urllib.parse import urlparse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ATHENA_DB = "healthcare_facility_db"
ATHENA_OUTPUT_LOCATION = "s3://healthcare-facility/athena_results/"

FINAL_RESULTS_BUCKET = "healthcare-facility"
FINAL_RESULTS_PREFIX = "transformed/"

athena = boto3.client("athena")
s3 = boto3.client("s3")

STATE_COUNTS_QUERY = """
SELECT 
    location.state,
    COUNT(DISTINCT facility_id) AS accredited_facility_count
FROM healthcare_facility_db.raw
WHERE cardinality(accreditations) > 0
GROUP BY location.state;
"""


def start_athena_query() -> str:
    """
    Start the Athena query and return the query execution ID.
    """
    logger.info("Using Athena output location: %s", ATHENA_OUTPUT_LOCATION)
    response = athena.start_query_execution(
        QueryString=STATE_COUNTS_QUERY,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    query_execution_id = response["QueryExecutionId"]
    logger.info("Started Athena query with execution id: %s", query_execution_id)
    return query_execution_id


def wait_for_query(query_execution_id: str, max_tries: int = 20, delay: int = 3) -> str:
    """
    Poll Athena until query finishes or fails.
    """
    for attempt in range(max_tries):
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]
        logger.info("Athena query state: %s (attempt %d)", state, attempt + 1)

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state

        time.sleep(delay)

    raise TimeoutError("Athena query timed out")


def copy_results_to_final_location(query_execution_id: str) -> str:
    """
    Copy the Athena results file from the temp output location
    into a final analytics prefix with a timestamped file name.
    Returns the final S3 path.
    """
    # Get execution details so we can find the output file
    execution = athena.get_query_execution(QueryExecutionId=query_execution_id)
    output_location = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    # output_location will look like: s3://bucket/path/to/file.csv

    parsed = urlparse(output_location)
    src_bucket = parsed.netloc
    src_key = parsed.path.lstrip("/")

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dest_key = f"{FINAL_RESULTS_PREFIX}state_counts_{timestamp}.csv"

    logger.info(
        "Copying Athena result from s3://%s/%s to s3://%s/%s",
        src_bucket,
        src_key,
        FINAL_RESULTS_BUCKET,
        dest_key,
    )

    copy_source = {"Bucket": src_bucket, "Key": src_key}
    s3.copy_object(Bucket=FINAL_RESULTS_BUCKET, Key=dest_key, CopySource=copy_source)

    return f"s3://{FINAL_RESULTS_BUCKET}/{dest_key}"


def lambda_handler(event, context):
    """
    Stage 3:
    Triggered by new S3 object (raw facility data).
    Runs Athena query to compute accredited facility counts per state,
    then copies the result to a final analytics prefix.
    """
    logger.info("Received event: %s", json.dumps(event))

    query_execution_id = start_athena_query()
    state = wait_for_query(query_execution_id)

    if state != "SUCCEEDED":
        logger.error("Athena query did not succeed. Final state: %s", state)
        raise RuntimeError(f"Athena query failed with state: {state}")

    final_s3_path = copy_results_to_final_location(query_execution_id)
    logger.info("State counts written to %s", final_s3_path)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Athena state counts query completed",
                "result_path": final_s3_path,
            }
        ),
    }
