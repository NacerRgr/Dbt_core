{{ config(materialized='view') }}

WITH diagnostics AS (
    SELECT
        id_diag,
        diag1,
        diag2,
        diag3,
        date_trunc('day', timestamp) AS date  -- Regroupement par jour
    FROM
        {{ ref('fct_diag') }}
),

diag1_agg AS (
    SELECT
        date,
        diag1 AS type_diagnostic,
        COUNT(*) AS nombre_diagnostics
    FROM
        diagnostics
    WHERE
        diag1 IS NOT NULL
    GROUP BY
        date, diag1
),

diag2_agg AS (
    SELECT
        date,
        diag2 AS type_diagnostic,
        COUNT(*) AS nombre_diagnostics
    FROM
        diagnostics
    WHERE
        diag2 IS NOT NULL
    GROUP BY
        date, diag2
),

diag3_agg AS (
    SELECT
        date,
        diag3 AS type_diagnostic,
        COUNT(*) AS nombre_diagnostics
    FROM
        diagnostics
    WHERE
        diag3 IS NOT NULL
    GROUP BY
        date, diag3
)

SELECT * FROM diag1_agg
UNION ALL
SELECT * FROM diag2_agg
UNION ALL
SELECT * FROM diag3_agg
ORDER BY date, type_diagnostic
