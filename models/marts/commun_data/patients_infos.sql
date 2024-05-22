{{ config(materialized='view') }}

SELECT
    id_patient,
    patient_name,
    patient_age,
    blood_type,
    gender
FROM
    {{ ref('dim_patient') }}
