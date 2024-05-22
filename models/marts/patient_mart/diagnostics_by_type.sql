{{ config(materialized='view') }}

-- Agrégation des diagnostics pour diag1
WITH diag1_agg AS (
    SELECT
        diag1 AS type_diagnostic,
        COUNT(*) AS nombre_diagnostics
    FROM
        {{ ref('fct_diag') }}
    WHERE
        diag1 IS NOT NULL
    GROUP BY
        diag1
),

-- Agrégation des diagnostics pour diag2
diag2_agg AS (
    SELECT
        diag2 AS type_diagnostic,
        COUNT(*) AS nombre_diagnostics
    FROM
        {{ ref('fct_diag') }}
    WHERE
        diag2 IS NOT NULL
    GROUP BY
        diag2
),

-- Agrégation des diagnostics pour diag3
diag3_agg AS (
    SELECT
        diag3 AS type_diagnostic,
        COUNT(*) AS nombre_diagnostics
    FROM
        {{ ref('fct_diag') }}
    WHERE
        diag3 IS NOT NULL
    GROUP BY
        diag3
)

-- Combinaison des agrégations
SELECT * FROM diag1_agg
UNION ALL
SELECT * FROM diag2_agg
UNION ALL
SELECT * FROM diag3_agg
ORDER BY nombre_diagnostics DESC
