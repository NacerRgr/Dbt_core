{{ config(materialized='table') }}

WITH source AS (
    SELECT
        ROW_NUMBER() OVER () AS id_mental, -- Génère un ID unique pour chaque ligne
        COALESCE(treatment, 'Unknown') AS treatment,
        COALESCE(family_history, 'Unknown') AS family_history,
        COALESCE(coping_struggles, 'Unknown') AS coping_struggles,
        COALESCE(mood_swings, 'Unknown') AS mood_swings,
        COALESCE(work_interest, 'Unknown')::VARCHAR AS work_interest,
        COALESCE(mental_health_interview, 'Unknown')::VARCHAR AS mental_health_interview,
        COALESCE(occupation, 'Unknown')::VARCHAR AS occupation,
        COALESCE(days_indoors, 'Unknown')::VARCHAR AS days_indoors,
        COALESCE(care_options, 'Unknown')::VARCHAR AS care_options,
        COALESCE(social_weakness, 'Unknown')::VARCHAR AS social_weakness,
        COALESCE(self_employed, 'Unknown') AS self_employed,
        COALESCE(country, 'Unknown')::VARCHAR AS country,
        COALESCE(changes_habits, 'Unknown') AS changes_habits,
        COALESCE(growing_stress, 'Unknown') AS growing_stress,
        COALESCE(mental_health_history, 'Unknown') AS mental_health_history,
        COALESCE(_airbyte_emitted_at, CURRENT_TIMESTAMP) as Time_stamp 

    FROM {{ source('medical_data', 'mental_health_dataset') }}
),

renamed AS (
    SELECT
        *
    FROM source
)

SELECT * FROM renamed
