{{ config(materialized='view') }}

WITH admissions AS (
    SELECT
        date_trunc('day', date_of_admission) AS date,
        COUNT(*) AS nombre_admissions
    FROM
        {{ ref('fct_admission') }}
    GROUP BY
        date
)

SELECT
    date,
    nombre_admissions
FROM
    admissions
ORDER BY
    date
