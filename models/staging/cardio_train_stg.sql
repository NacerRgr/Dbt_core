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
        CAST(id AS INT) as id_cardio_train, -- Renommer ici
        CAST(height AS INT) as height,
        CAST(age AS INT)/365 as age_years -- Convertit l'âge en années
    FROM {{ source('medical_data', 'cardio_train') }}
), 

renamed as (
    SELECT
        gluc,
        gender,
        smoke,
        active,
        weight,
        cardio,
        ap_lo,
        cholesterol,
        alco,
        ap_hi,
        id_cardio_train, 
        height,
        age_years
    FROM source
)

select * from renamed