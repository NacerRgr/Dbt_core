-- models/fact/fct_traitement_maladie.sql
{{ config(materialized='table') }}

WITH treatment_details AS (
    SELECT
        md.id_dossier_medical AS id_trt_maladie,
        pat.id_patient,
        md.maladie,
        md.solution AS traitement_a_faire,
        md.results AS resultat_test,
        doc.doctor_id AS id_docteur,
        CURRENT_TIMESTAMP AS time_stamp  -- Assuming you want to record the current timestamp
    FROM {{ ref('dossier_med_dim') }} md
    JOIN {{ ref('patient_dim') }} pat ON pat.id_patient = md.id_dossier_medical  -- Assuming id_dossier_medical corresponds to patient ID
    JOIN {{ ref('doctor_dim') }} doc ON doc.doctor_id = md.id_dossier_medical  -- Assuming id_dossier_medical corresponds to doctor ID
    -- Note: The JOIN conditions above are likely incorrect and need to be revised based on your actual data relationships.
    -- The `id_dossier_medical` should match the correct foreign key in the patient and doctor dimension tables.
)

SELECT
    id_trt_maladie,
    id_patient,
    -- id_dossier_medical should not be selected again since it is already aliased as id_trt_maladie
    id_docteur,
    resultat_test,
    traitement_a_faire,
    time_stamp
FROM treatment_details
