{{ config(materialized='table') }}

WITH source AS (
    SELECT
        ROW_NUMBER() OVER () AS id_readmission,  -- Génère un ID unique pour chaque ligne
        CASE 
            WHEN readmitted = 'no' THEN 'no'
            WHEN readmitted = 'yes' THEN 'yes'
            ELSE 'unknown'  -- Gestion des valeurs inattendues avec un fallback sûr
        END as readmitted_enum,
        COALESCE(CAST(n_inpatient AS INT), 0) as n_inpatient,
        COALESCE(diag_1, 'Not Specified')::VARCHAR as diag_1,
        COALESCE(CAST(n_lab_procedures AS INT), 0) as n_lab_procedures,
        COALESCE(diag_3, 'Not Specified')::VARCHAR as diag_3,
        COALESCE(CAST(n_outpatient AS INT), 0) as n_outpatient,
        CASE 
            WHEN change = 'no' THEN 'no'
            WHEN change = 'yes' THEN 'yes'
            ELSE 'unknown'  -- Gestion des valeurs inattendues avec un fallback sûr
        END as change_enum,
        COALESCE(diag_2, 'Not Specified')::VARCHAR as diag_2,
        CASE 
            WHEN diabetes_med = 'no' THEN 'no'
            WHEN diabetes_med = 'yes' THEN 'yes'
            ELSE 'unknown'  -- Gestion des valeurs inattendues avec un fallback sûr
        END as diabetes_med_enum,
        COALESCE(CAST(time_in_hospital AS INT), 0) as time_in_hospital,
        COALESCE(medical_specialty, 'Not Specified')::VARCHAR as medical_specialty,
        CASE 
            WHEN glucose_test = 'no' THEN 'no'
            WHEN glucose_test = 'yes' THEN 'yes'
            ELSE 'unknown'  -- Gestion des valeurs inattendues avec un fallback sûr
        END as glucose_test_enum,
        COALESCE(CAST(n_procedures AS INT), 0) as n_procedures,
        COALESCE(a1ctest, 'Not Specified')::VARCHAR as a1ctest,
        COALESCE(age, 'Unknown')::VARCHAR as age,
        COALESCE(CAST(n_emergency AS INT), 0) as n_emergency,
        COALESCE(CAST(n_medications AS INT), 0) as n_medications
    FROM {{ source('medical_data', 'hospital_readmissions') }}
),

renamed AS (
    SELECT
        *
    FROM source
    WHERE glucose_test_enum != 'unknown'
)

SELECT * FROM renamed
