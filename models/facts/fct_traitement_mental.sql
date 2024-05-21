-- models/fact/fct_visite_urgence.sql
{{ config(materialized='table') }}

WITH mental_health AS (
   select id_mental as id_mental_health , 
   id_patient ,
   treatment as treatment , 
   growing_stress as stress , 
   mood_swings as  mood_swings,
   social_weakness as social_weakness,
   Time_stamp as time_stamp
   from {{ ref('mental_health_stg') }} mth 
   join {{ref('dim_patient')}} pt on mth.id_mental = pt.id_patient
   where mth.treatment is not null and 
   mth.growing_stress is not null and
   mth.mood_swings is not null and
   mth.social_weakness is not null and
   mth.id_mental is not null 
)


select  * from  mental_health 


