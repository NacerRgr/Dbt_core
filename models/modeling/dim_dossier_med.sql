-- models/dimension/dim_dossier_medical.sql
{{ config(materialized='table') }}

WITH dossier_medical AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY id_healthcare) AS id_dossier_medical,
        medical_condition AS maladie,
        medication AS solution,
        test_results AS results
    FROM {{ ref('healthcare_dataset_stg') }}
    WHERE 
        medical_condition IS NOT NULL AND
        medication IS NOT NULL AND
        test_results IS NOT NULL
),

Distint as (
    select DISTINCT 
    id_dossier_medical,
    maladie,
    solution,
    results
    from dossier_medical
)


select * from Distint