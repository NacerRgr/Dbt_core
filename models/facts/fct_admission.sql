-- models/fact/fct_admission.sql
{{ config(materialized='table') }}

WITH admissions AS (
    SELECT
        hds.id_healthcare AS id_admission,
        hds.time_stamp AS timestamp,
        hds.billing_amount,
        hds.discharge_date,
        hds.room_number,
        CASE
            WHEN hds.gender_enum = 1 THEN 'Male'
            WHEN hds.gender_enum = 2 THEN 'Female'
            ELSE 'Other' -- ou 'Unknown'; dépend de vos données
        END AS gender,
        hds.date_of_admission,
        hds.age,
        hds.medical_condition,
        hds.insurance_provider,
        pdim.id_patient,
        dd.doctor_id,
        hdim.hospital_id,
        rdv.rdv_id,
        ctr.weight AS patient_weight,
        ctr.cardio AS cardio_condition,
        hrt.cp AS chest_pain_type,
        mhst.treatment AS mental_health_treatment,
        rhst.n_medications AS number_of_medications
    FROM {{ ref('healthcare_dataset_stg') }} hds
    LEFT JOIN {{ ref('patient_dim') }} pdim ON hds.id_healthcare = pdim.patient_id_from_stg
    LEFT JOIN {{ ref('doctor_dim') }} dd ON hds.doctor = dd.doctor_name -- Assurez-vous que la jointure se fait avec la bonne colonne
    LEFT JOIN {{ ref('hospital_dim') }} hdim ON hds.hospital = hdim.hospital_name -- Assurez-vous que la jointure se fait avec la bonne colonne
    LEFT JOIN {{ ref('rdv_dim') }} rdv ON hds.id_healthcare = rdv.rdv_id -- Assurez-vous que la jointure se fait avec la bonne colonne
    LEFT JOIN {{ ref('cardio_train_stg') }} ctr ON hds.id_healthcare = ctr.id_cardio_train
    LEFT JOIN {{ ref('heart_stg') }} hrt ON hds.id_healthcare = hrt.id_heart
    LEFT JOIN {{ ref('mental_health_stg') }} mhst ON hds.id_healthcare = mhst.id_mental
    LEFT JOIN {{ ref('readmission_hospital_stg') }} rhst ON hds.id_healthcare = rhst.id_readmisson
    WHERE
        hds.id_healthcare IS NOT NULL
        AND hds.time_stamp IS NOT NULL
        AND hds.billing_amount IS NOT NULL
        AND hds.discharge_date IS NOT NULL
        AND hds.room_number IS NOT NULL
        AND hds.gender_enum IS NOT NULL
        AND hds.date_of_admission IS NOT NULL
        AND hds.age IS NOT NULL
        AND hds.medical_condition IS NOT NULL
        AND hds.insurance_provider IS NOT NULL
        AND ctr.id_cardio_train IS NOT NULL
        AND ctr.weight IS NOT NULL
        AND ctr.cardio IS NOT NULL
        AND hrt.id_heart IS NOT NULL
        AND hrt.cp IS NOT NULL
        AND mhst.id_mental IS NOT NULL
        AND mhst.treatment IS NOT NULL
        AND rhst.id_readmisson IS NOT NULL
        AND rhst.n_medications IS NOT NULL
)

SELECT
    id_admission,
    timestamp,
    billing_amount,
    discharge_date,
    room_number,
    gender,
    date_of_admission,
    age,
    medical_condition,
    insurance_provider,
    id_patient,
    doctor_id,
    hospital_id,
    rdv_id,
    patient_weight,
    cardio_condition,
    chest_pain_type,
    mental_health_treatment,
    number_of_medications
FROM admissions
