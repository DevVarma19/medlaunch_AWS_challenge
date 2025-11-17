import logging
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta


import boto3
from botocore.exceptions import ClientError, BotoCoreError

# config settings

# AWS session profile and region
AWS_PROFILE = "devvarma-mac"
AWS_REGION = "us-east-1"

# input and output buckets and keys
RAW_BUCKET = "healthcare-facility"
TRANSFORMED_BUCKET = "healthcare-facility"

RAW_KEY = "raw/sample_facility_data.json"
TRANSFORMED_KEY = "transformed/expiring_facilities.json"

# logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# utility functions
def parse_json_lines(body):
    facilities = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            facilities.append(json.loads(line))
        except json.JSONDecodeError:
            print(f"Error parsing JSON line: {line}")
            continue
    return facilities

def check_if_expiring(valid_until):
    """
    Check if the accreditation is expiring in the next 6 months
    Args:
        valid_until: Date of the accreditation
    Returns:
        True if the accreditation is expiring in the next 6 months, False otherwise
    """
    try:
        return datetime.fromisoformat(valid_until).date() <= (datetime.now().date() + relativedelta(months=6))
    except Exception as e:
        logger.error(f"Error checking if accreditation is expiring: {e} for valid_until: {valid_until}")
        return False

# botocore functions
def get_s3_client():
    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION
    )
    return session.client("s3")

def write_to_s3(s3_client, data):
    """
    Write the data to the S3 bucket
    Args:
        s3_client: S3 client
        data: Data to write
    Returns:
        None
    """
    try:
        logger.info(f"Writing {len(data)} facilities to s3://{TRANSFORMED_BUCKET}/{TRANSFORMED_KEY}")
        s3_client.put_object(Bucket=TRANSFORMED_BUCKET, Key=TRANSFORMED_KEY, Body=json.dumps(data))
    except ClientError as e:
        logger.error(f"Failed to write to s3://{TRANSFORMED_BUCKET}/{TRANSFORMED_KEY}: {e}")
    except Exception as e:
        logger.error(f"Error writing to s3://{TRANSFORMED_BUCKET}/{TRANSFORMED_KEY}: {e}")
    return None

def get_all_facilities(s3_client):
    """
    Get all facilities from the S3 bucket
    Args:
        s3_client: S3 client
    Returns:
        List of facilities
    """
    try:
        logger.info(f"Reading input file s3://{RAW_BUCKET}/{RAW_KEY}")
        resp = s3_client.get_object(Bucket=RAW_BUCKET, Key=RAW_KEY)
        body = resp["Body"].read().decode("utf-8").strip()
        facilities = parse_json_lines(body)
        logger.info(f"Found {len(facilities)} facilities in the file s3://{RAW_BUCKET}/{RAW_KEY}")    
        return facilities
    except ClientError as e:
        logger.error(f"Failed to read input file s3://{RAW_BUCKET}/{RAW_KEY}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error getting all facilities: {e}")
        return []

def filter_expiring_facilities(facilities):
    """
    Filter expiring facilities from the list of facilities
    Args:
        facilities: List of facilities
    Returns:
        List of expiring facilities
    """
    logger.info(f"Filtering facilities with accreditation expiring in the next 6 months on or before {datetime.now().date() + relativedelta(months=6)}")
    expiring_facilities = []
    for facility in facilities:
        accreditations = facility.get("accreditations", [])
        for acc in accreditations:
            if check_if_expiring(acc.get("valid_until")):
                logger.info(f"Accreditation of {facility.get('facility_name')} with {acc.get('accreditation_body')} is expiring on {acc.get('valid_until')}")
                expiring_facilities.append(facility)
                break
    logger.info(f"Found {len(expiring_facilities)} facilities with accreditation expiring in the next 6 months on or before {datetime.now().date() + relativedelta(months=6)}")
    return expiring_facilities

if __name__ == "__main__":
  logger.info("Starting the transformation of the data")
  s3_client = get_s3_client()
  facilities = get_all_facilities(s3_client)
  expiring_facilities = filter_expiring_facilities(facilities)
  write_to_s3(s3_client, expiring_facilities)
  logger.info("Transformation completed successfully")