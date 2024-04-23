-- models/dimension/dim_hospital.sql
{{ config(materialized='table') }}

WITH hospital_details AS (
    SELECT
        hospital AS hospital_name,
        COUNT(DISTINCT id_healthcare) AS patient_count,
        COUNT(DISTINCT room_number) AS room_count
    FROM {{ ref('healthcare_dataset_stg') }}
    GROUP BY hospital
)

SELECT
    ROW_NUMBER() OVER (ORDER BY hospital_name) AS hospital_id,
    hospital_name,
    patient_count,
    room_count
FROM hospital_details


