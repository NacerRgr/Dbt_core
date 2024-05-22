{{ config(materialized='view') }}

WITH mental_treatments AS (
    SELECT
        id_mental_health,
        id_patient,
        treatment,
        date_trunc('day', time_stamp) AS date  -- Regroupement par jour (peut être ajusté selon les besoins)
    FROM
        {{ ref('fct_traitement_mental') }}
    WHERE
        treatment IS NOT NULL
),

treatment_agg AS (
    SELECT
        date,
        treatment AS type_treatment,
        COUNT(*) AS nombre_treatments
    FROM
        mental_treatments
    GROUP BY
        date, treatment
)

SELECT
    date,
    type_treatment,
    nombre_treatments
FROM
    treatment_agg
ORDER BY
    date, type_treatment
