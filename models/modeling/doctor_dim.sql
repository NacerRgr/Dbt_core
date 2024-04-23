-- models/dimension/dim_doctor.sql
{{ config(materialized='table') }}

WITH doctor_details AS (
    SELECT
        doctor AS doctor_name,
        medical_condition as doctor_specialty,
        _airbyte_emitted_at AS  Time_stamp
    FROM {{ ref('healthcare_dataset_stg') }}
),

unique_doctors AS (
    SELECT DISTINCT
        doctor_name,
        doctor_specialty
        Time_stamp
    FROM doctor_details
)

SELECT
    ROW_NUMBER() OVER (ORDER BY doctor_name) AS doctor_id,
    doctor_name,
    doctor_specialty,
    Time_stamp
FROM unique_doctors
