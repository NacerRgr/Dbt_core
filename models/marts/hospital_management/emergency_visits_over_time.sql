{{ config(materialized='view') }}

WITH emergency_visits AS (
    SELECT
        date_trunc('day', date_visite) AS date,
        COUNT(*) AS nombre_visites
    FROM
        {{ ref('fct_visite_urgence') }}
    GROUP BY
        date
)

SELECT
    date,
    nombre_visites
FROM
    emergency_visits
ORDER BY
    date
