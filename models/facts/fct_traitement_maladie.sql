-- models/fact/fct_traitement_maladie.sql
{{ config(materialized='table') }}

WITH traitement_details AS (
    SELECT
        ROW_NUMBER() OVER() AS id_trt_maladie,  -- Génère un ID unique pour chaque traitement
        pd.id_patient,
        dm.id_dossier_medical,
        doc.doctor_id AS id_docteur,
        dm.results AS resultat_test,
        dm.solution AS traitement_a_faire,
        CURRENT_TIMESTAMP AS time_stamp  -- Enregistrement du timestamp actuel
    FROM {{ ref('dim_dossier_med') }} dm
    JOIN {{ ref('dim_patient') }} pd ON dm.id_dossier_medical = pd.id_patient  -- Supposons cette relation pour l'exemple
    JOIN {{ ref('dim_doctor') }} doc ON dm.id_dossier_medical = doc.doctor_id  -- Supposons cette relation pour l'exemple
    WHERE
        dm.id_dossier_medical IS NOT NULL AND
        pd.id_patient IS NOT NULL AND
        doc.doctor_id IS NOT NULL
)

SELECT
    id_trt_maladie,
    id_patient,
    id_dossier_medical,
    id_docteur,
    resultat_test,
    traitement_a_faire,
    time_stamp
FROM traitement_details
