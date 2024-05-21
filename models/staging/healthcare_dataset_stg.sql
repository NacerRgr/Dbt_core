{{ config(materialized='table') }}

WITH source AS (
    SELECT
        ROW_NUMBER() OVER() as id_healthcare,
        COALESCE(CAST("Billing Amount" AS DECIMAL), 0) as billing_amount,
        COALESCE("Discharge Date"::DATE, CURRENT_DATE) as discharge_date,
        COALESCE(CAST("Room Number" AS INT), 0) as room_number,
        COALESCE("Test Results"::VARCHAR, '') as test_results,
        COALESCE(doctor::VARCHAR, '') as doctor,
        CASE 
            WHEN gender = 'Male' THEN 0
            WHEN gender = 'Female' THEN 1
            ELSE 1111 -- Utiliser 0 pour les valeurs inconnues ou non spécifiées
        END as gender_enum,
        COALESCE("Admission Type"::VARCHAR, '') as admission_type,
        COALESCE("Date of Admission"::DATE, CURRENT_DATE) as date_of_admission,
        COALESCE("Insurance Provider"::VARCHAR, '') as insurance_provider,
        COALESCE("Name"::VARCHAR, '') as name,
        CASE 
            WHEN "Blood Type" IN ('A', 'B', 'AB', 'O', 'A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-') THEN "Blood Type"
            ELSE 'Unknown'
        END as blood_type_enum,
        COALESCE(medication::VARCHAR, '') as medication,
        COALESCE(hospital::VARCHAR, '') as hospital,
        COALESCE("Medical Condition"::VARCHAR, '') as medical_condition,
        COALESCE(CAST(age AS INT), 0) as age,
        COALESCE(_airbyte_emitted_at, CURRENT_TIMESTAMP) as Time_stamp 
    FROM {{ source('medical_data', 'healthcare_dataset') }}
),

renamed AS (
SELECT
    *
FROM source
)

select * from renamed
