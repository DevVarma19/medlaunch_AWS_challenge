SELECT 
    facility_id,
    facility_name,
    employee_count,
    cardinality(services) AS number_of_offered_services,
    accreditations[1].valid_until as expiry_date_of_first_accreditation
FROM "healthcare_facility_db"."raw";