-- models/dimension/dim_rendez_vous.sql
{{ config(materialized='table') }}

WITH rendez_vous AS (
    SELECT
        hd.id_healthcare AS rdv_id,
        pat.id_patient AS patient_id,
        doc.doctor_id AS doctor_id,
        hosp.hospital_id AS hospital_id,
        hd.date_of_admission AS rdv_date,
        hd.admission_type AS rdv_type
    FROM {{ ref('healthcare_dataset_stg') }} hd
    LEFT JOIN {{ ref('patient_dim') }} pat ON hd.id_healthcare = pat.patient_id_from_stg
    LEFT JOIN {{ ref('doctor_dim') }} doc ON hd.doctor = doc.doctor_name
    LEFT JOIN {{ ref('hospital_dim') }} hosp ON hd.hospital = hosp.hospital_name
)

SELECT
    rdv_id,
    patient_id,
    doctor_id,
    hospital_id,
    rdv_date,
    rdv_type
FROM rendez_vous
