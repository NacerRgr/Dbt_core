{{ config(materialized='table') }}



WITH source AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as id_heart,  -- Ajout d'un ID unique pour chaque ligne
        CAST(exang AS INT) as exang,
        CAST(sex AS INT) as sex,
        CAST(thal AS INT) as thal,
        CAST(chol AS INT) as chol,
        CAST(slope AS INT) as slope,
        CAST(cp AS INT) as cp,
        CAST(target AS INT) as target,
        CAST(trestbps AS INT) as trestbps,
        CAST(oldpeak AS FLOAT) as oldpeak,
        CAST(thalach AS INT) as thalach,
        CAST(fbs AS INT) as fbs,
        CAST(ca AS INT) as ca,
        CAST(age AS INT) as age,
        CAST(restecg AS INT) as restecg
    FROM {{ source('medical_data', 'heart') }} -- Remplacez par les noms appropriés de schéma et de table
),


renamed AS (
SELECT
    *
FROM source
)

select * from renamed
