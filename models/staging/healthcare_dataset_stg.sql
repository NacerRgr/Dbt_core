{{ config(materialized='table') }}

WITH source AS (
    SELECT
        ROW_NUMBER() OVER() as id_healthcare, -- Génère un ID unique pour chaque ligne
        CAST("Billing Amount" AS DECIMAL) as billing_amount,
        "Discharge Date"::DATE as discharge_date,
        CAST("Room Number" AS INT) as room_number,
        "Test Results"::VARCHAR as test_results,
        doctor::VARCHAR as doctor,
        CASE 
            WHEN gender = 'male' THEN 1
            WHEN gender = 'female' THEN 2
            ELSE NULL -- Gérer les cas inattendus
        END as gender_enum,
        "Admission Type"::VARCHAR as admission_type,
        "Date of Admission"::DATE as date_of_admission,
        "Insurance Provider"::VARCHAR as insurance_provider,
        "Name"::VARCHAR as name,
        CASE 
            -- Convertir les types sanguins en une énumération spécifique, si nécessaire
            WHEN "Blood Type" IN ('A', 'B', 'AB', 'O', 'A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-') THEN "Blood Type"
            ELSE NULL -- Gérer les cas inattendus
        END as blood_type_enum,
        medication::VARCHAR as medication,
        hospital::VARCHAR as hospital,
        "Medical Condition"::VARCHAR as medical_condition,
        CAST(age AS INT) as age
    FROM {{ source('medical_data', 'healthcare_dataset') }}
),

renamed AS (
SELECT
    *
FROM source
)

select * from renamed

