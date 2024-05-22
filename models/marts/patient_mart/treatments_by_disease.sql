{{ config(materialized='view') }}

WITH treatments AS (
    SELECT
        dm.maladie,
        tm.traitement_a_faire,
        date_trunc('day', tm.time_stamp) AS date  -- Regroupement par jour (peut être ajusté selon les besoins)
    FROM
        {{ ref('fct_traitement_maladie') }} tm
    JOIN
        {{ ref('dim_dossier_med') }} dm ON tm.id_dossier_medical = dm.id_dossier_medical
    WHERE
        dm.maladie IS NOT NULL
        AND tm.traitement_a_faire IS NOT NULL
),

treatment_agg AS (
    SELECT
        maladie AS type_maladie,
        traitement_a_faire AS type_traitement,
        COUNT(*) AS nombre_traitements
    FROM
        treatments
    GROUP BY
        maladie, traitement_a_faire
)

SELECT
    type_maladie,
    type_traitement,
    nombre_traitements
FROM
    treatment_agg
ORDER BY
    type_maladie, nombre_traitements DESC
