{{ config(materialized='table') }}


WITH source AS (
    SELECT
        ROW_NUMBER() OVER () AS id_mental, -- Génère un ID unique pour chaque ligne
        CASE treatment
            WHEN 'Yes' THEN 'Yes'
            WHEN 'No' THEN 'No'
            ELSE NULL
        END as treatment,
        CASE family_history
            WHEN 'Yes' THEN 'Yes'
            WHEN 'No' THEN 'No'
            ELSE NULL
        END as family_history,
        CASE coping_struggles
            WHEN 'Yes' THEN 'Yes'
            WHEN 'No' THEN 'No'
            ELSE NULL
        END as coping_struggles,
        CASE mood_swings
            WHEN 'Low' THEN 'Low'
            WHEN 'Medium' THEN 'Medium'
            WHEN 'High' THEN 'High'
            ELSE NULL
        END as mood_swings,
        work_interest::VARCHAR as work_interest,
        mental_health_interview::VARCHAR as mental_health_interview,
        occupation::VARCHAR as occupation,
        days_indoors::VARCHAR as days_indoors,
        care_options::VARCHAR as care_options,
        social_weakness::VARCHAR as social_weakness,
        CASE self_employed
            WHEN 'Yes' THEN 'Yes'
            WHEN 'No' THEN 'No'
            ELSE NULL
        END as self_employed,
        country::VARCHAR as country,
        CASE changes_habits
            WHEN 'Yes' THEN 'Yes'
            WHEN 'No' THEN 'No'
            ELSE NULL
        END as changes_habits,
        CASE growing_stress
            WHEN 'Yes' THEN 'Yes'
            WHEN 'No' THEN 'No'
            ELSE NULL
        END as growing_stress,
        CASE mental_health_history
            WHEN 'Yes' THEN 'Yes'
            WHEN 'No' THEN 'No'
            ELSE NULL
        END as mental_health_history
    FROM {{ source('medical_data', 'mental_health_dataset') }}
),


renamed AS (
SELECT
    *
FROM source
)

select * from renamed

