{{ config(materialized='view') }}

WITH treatment_types AS (
    SELECT
        traitement_a_faire AS type_traitement,
        COUNT(*) AS nombre_traitements
    FROM
        {{ ref('fct_traitement_maladie') }}
    WHERE
        traitement_a_faire IS NOT NULL
    GROUP BY
        traitement_a_faire
)

SELECT
    type_traitement,
    nombre_traitements
FROM
    treatment_types
ORDER BY
    nombre_traitements DESC
