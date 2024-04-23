{{ config(materialized='table') }}


WITH source AS (
    SELECT
        ROW_NUMBER() OVER () AS{{ config(materialized='table') }}
 id_readmisson, -- Ajoute d'un identifiant unique pour chaque ligne
        CASE 
            WHEN readmitted = 'no' THEN 'no'
            WHEN readmitted = 'yes' THEN 'yes'
            ELSE NULL -- Gestion des valeurs inattendues
        END as readmitted_enum,
        CAST(n_inpatient AS INT) as n_inpatient,
        diag_1::VARCHAR as diag_1,
        CAST(n_lab_procedures AS INT) as n_lab_procedures,
        diag_3::VARCHAR as diag_3,
        CAST(n_outpatient AS INT) as n_outpatient,
        CASE 
            WHEN change = 'no' THEN 'no'
            WHEN change = 'yes' THEN 'yes'
            ELSE NULL -- Gestion des valeurs inattendues
        END as change_enum,
        diag_2::VARCHAR as diag_2,
        CASE 
            WHEN diabetes_med = 'no' THEN 'no'
            WHEN diabetes_med = 'yes' THEN 'yes'
            ELSE NULL -- Gestion des valeurs inattendues
        END as diabetes_med_enum,
        CAST(time_in_hospital AS INT) as time_in_hospital,
        medical_specialty::VARCHAR as medical_specialty,
        CASE 
            WHEN glucose_test = 'no' THEN 'no'
            WHEN glucose_test = 'yes' THEN 'yes'
            ELSE NULL -- Gestion des valeurs inattendues
        END as glucose_test_enum,
        CAST(n_procedures AS INT) as n_procedures,
        a1ctest::VARCHAR as a1ctest,
        age::VARCHAR as age, -- Supposé qu'il n'est pas nécessaire de convertir en int
        CAST(n_emergency AS INT) as n_emergency,
        CAST(n_medications AS INT) as n_medications
    FROM {{ source('medical_data', 'hospital_readmissions') }}
),


renamed AS (
SELECT
    *
FROM source     
WHERE glucose_test_enum IS NOT NULL
)

select * from renamed

