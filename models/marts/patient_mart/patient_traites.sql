{{ config(materialized='view') }}

WITH patients_treated AS (
    SELECT
        id_patient,
        COUNT(*) AS nombre_traitements
    FROM
        {{ ref('fct_traitement_mental') }}
    GROUP BY
        id_patient
)

SELECT
    COUNT(id_patient) AS total_patients_treated,
    SUM(nombre_traitements) AS total_treatments
FROM
    patients_treated
