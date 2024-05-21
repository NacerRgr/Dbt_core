-- models/fact/fct_admission.sql
{{ config(materialized='table') }}

WITH admissions AS (
    SELECT 
        ROW_NUMBER() OVER() AS id_admission,
        pdim.id_patient,
        hdim.hospital_id,
        dd.doctor_id,
        hds.date_of_admission,
        hds.time_stamp  
    FROM {{ ref('healthcare_dataset_stg') }} hds
    LEFT JOIN {{ ref('dim_patient') }} pdim ON hds.id_healthcare = pdim.id_patient  -- Corrected to use the right reference from patient_dim
    LEFT JOIN {{ ref('dim_doctor') }} dd ON hds.doctor = dd.doctor_name  -- Assuming doctor matching is by name
    LEFT JOIN {{ ref('dim_hospital') }} hdim ON hds.hospital = hdim.hospital_name  -- Assuming hospital matching is by name
    WHERE
        hds.id_healthcare IS NOT NULL
        AND hds.date_of_admission IS NOT NULL
        AND pdim.id_patient IS NOT NULL
        AND dd.doctor_id IS NOT NULL
        AND hdim.hospital_id IS NOT NULL
)

SELECT
    id_admission,
    id_patient,
    hospital_id,
    doctor_id,
    date_of_admission,
    time_stamp  
FROM admissions
