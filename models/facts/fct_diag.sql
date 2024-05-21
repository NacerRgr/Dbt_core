{{ config(materialized='table') }}

WITH diagnostics AS (
    SELECT
        ROW_NUMBER() OVER () AS id_diag,  -- Génère un ID unique pour chaque diagnostic
        pat.id_patient,
        doc.doctor_id AS id_doctor,
        hosp.hospital_id AS id_hospital,
        rhst.diag_1 AS diag1,  
        rhst.diag_2 AS diag2,
        rhst.diag_3 AS diag3,
        CURRENT_TIMESTAMP AS timestamp  -- Enregistrement du timestamp actuel
    FROM {{ ref('readmission_hospital_stg') }} rhst
    JOIN {{ ref('dim_patient') }} pat ON rhst.id_readmission = pat.id_patient
    JOIN {{ ref('dim_doctor') }} doc ON rhst.id_readmission = doc.doctor_id
    JOIN {{ ref('dim_hospital') }} hosp ON rhst.id_readmission = hosp.hospital_id
    WHERE
        rhst.diag_1 IS NOT NULL 
)

SELECT
    id_diag,
    id_patient,
    id_doctor,
    id_hospital,
    diag1,
    diag2,
    diag3,
    timestamp
FROM diagnostics
