-- models/dimension/dim_patient.sql
{{ config(materialized='table') }}

WITH source AS (
    SELECT DISTINCT
        ROW_NUMBER() OVER() as id_patient,
        name AS patient_name,
        age AS patient_age,
        blood_type_enum AS blood_type,
        gender_enum AS gender
    FROM {{ ref('healthcare_dataset_stg') }}
),

renamed AS (
SELECT
    *
FROM source
)

select * from renamed
