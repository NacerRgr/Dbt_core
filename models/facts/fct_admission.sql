-- models/fact/fct_admission.sql
{{ config(materialized='table') }}

WITH admissions AS (
    SELECT
        hds.id_healthcare AS id_admission,
        pdim.id_patient,
        hdim.hospital_id,
        dd.doctor_id,
        hds.date_of_admission,
        hds.room_number,
        hds.billing_amount,
        hds.time_stamp  -- Added to include the timestamp from the staging data
    FROM {{ ref('healthcare_dataset_stg') }} hds
    LEFT JOIN {{ ref('patient_dim') }} pdim ON hds.id_healthcare = pdim.patient_id_from_stg  -- Corrected to use the right reference from patient_dim
    LEFT JOIN {{ ref('doctor_dim') }} dd ON hds.doctor = dd.doctor_name  -- Assuming doctor matching is by name
    LEFT JOIN {{ ref('hospital_dim') }} hdim ON hds.hospital = hdim.hospital_name  -- Assuming hospital matching is by name
    WHERE
        hds.id_healthcare IS NOT NULL
        AND hds.date_of_admission IS NOT NULL
        AND hds.room_number IS NOT NULL
        AND hds.billing_amount IS NOT NULL
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
    room_number,
    billing_amount,
    time_stamp  
FROM admissions
