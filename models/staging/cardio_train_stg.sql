-- models/staging/cardio_train_stg.sql
{{ config(materialized='table') }}

WITH source AS (
    SELECT
        CAST(gluc AS INT) as gluc,
        CAST(gender AS INT) as gender,
        CAST(smoke AS INT) as smoke,
        CAST(active AS INT) as active,
        CAST(weight AS FLOAT) as weight,
        CAST(cardio AS INT) as cardio,
        CAST(ap_lo AS INT) as ap_lo,
        CAST(cholesterol AS INT) as cholesterol,
        CAST(alco AS INT) as alco,
        CAST(ap_hi AS INT) as ap_hi,
        CAST(id AS INT) as id_cardio_train,
        CAST(height AS INT) as height,
        CAST(age AS INT) / 365 as age_years
    FROM {{ source('medical_data', 'cardio_train') }}
    WHERE gluc IS NOT NULL AND
          gender IS NOT NULL AND
          smoke IS NOT NULL AND
          active IS NOT NULL AND
          weight IS NOT NULL AND
          cardio IS NOT NULL AND
          ap_lo IS NOT NULL AND
          cholesterol IS NOT NULL AND
          alco IS NOT NULL AND
          ap_hi IS NOT NULL AND
          id IS NOT NULL AND
          height IS NOT NULL AND
          age IS NOT NULL
)

SELECT *
FROM source