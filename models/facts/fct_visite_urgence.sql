-- models/fact/fct_visite_urgence.sql
{{ config(materialized='table') }}

WITH visite_urgence AS (
    SELECT
        ROW_NUMBER() OVER()  AS id_visite_urgence,  -- Génère un ID unique pour chaque visite
        rdv.patient_id AS id_patient,
        hosp.hospital_id AS id_hopital,
        rdv.rdv_date AS date_visite,
        CURRENT_TIMESTAMP AS time_stamp  -- Enregistrement du timestamp actuel
    FROM {{ ref('readmission_hospital_stg') }} rhst
    JOIN {{ ref('dim_rdv') }} rdv ON rhst.id_readmission = rdv.rdv_id
    JOIN {{ ref('dim_hospital') }} hosp ON rdv.hospital_id = hosp.hospital_id
    WHERE
        rhst.id_readmission IS NOT NULL AND
        rdv.rdv_id IS NOT NULL AND
        hosp.hospital_id IS NOT NULL
)

SELECT
    id_visite_urgence,
    id_patient,
    id_hopital,
    date_visite,
    time_stamp
FROM visite_urgence
