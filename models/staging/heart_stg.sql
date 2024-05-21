{{ config(materialized='table') }}

WITH source AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as id_heart,  -- Ajout d'un ID unique pour chaque ligne
        COALESCE(CAST(exang AS INT), 0) as exang,
        COALESCE(CAST(sex AS INT), 0) as sex,
        COALESCE(CAST(thal AS INT), 0) as thal,
        COALESCE(CAST(chol AS INT), 0) as chol,
        COALESCE(CAST(slope AS INT), 0) as slope,
        COALESCE(CAST(cp AS INT), 0) as cp,
        COALESCE(CAST(target AS INT), 0) as target,
        COALESCE(CAST(trestbps AS INT), 0) as trestbps,
        COALESCE(CAST(oldpeak AS FLOAT), 0.0) as oldpeak,
        COALESCE(CAST(thalach AS INT), 0) as thalach,
        COALESCE(CAST(fbs AS INT), 0) as fbs,
        COALESCE(CAST(ca AS INT), 0) as ca,
        COALESCE(CAST(age AS INT), 0) as age,
        COALESCE(CAST(restecg AS INT), 0) as restecg
    FROM {{ source('medical_data', 'heart') }} 
),

renamed AS (
    SELECT
        *
    FROM source
)

SELECT * FROM renamed
