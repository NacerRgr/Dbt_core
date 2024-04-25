-- models/fact/fct_visite_urgence.sql
{{ config(materialized='table') }}

WITH visite_urgence AS (
    SELECT
        rhst.id_readmisson AS id_urgent_visite,
        rdv.patient_id AS id_patient,
        rdv.hospital_id AS id_hopital,  -- Ajout de l'identifiant de l'hôpital directement à partir de rdv_dim
        rdv.rdv_date AS date_visite,
        rdv.rdv_type AS type_visite,
        rhst.time_in_hospital AS duree_hospitalisation,  -- Assurez-vous que cette colonne est correcte
        rhst.medical_specialty AS specialite_medicale,
        rhst.diag_1,
        rhst.diag_2,
        rhst.diag_3,
        rhst.n_emergency AS nb_urgences,
        rhst.n_medications AS nb_medicaments,
        rdv.rdv_date AS admission_date,  -- Utilisation de rdv_date pour admission_date
        rdv.rdv_type AS admission_type,  -- Utilisation de rdv_type pour admission_type
        CURRENT_TIMESTAMP AS time_stamp  -- Utilisation du timestamp actuel
    FROM {{ ref('readmission_hospital_stg') }} rhst
    JOIN {{ ref('rdv_dim') }} rdv ON rhst.id_readmisson = rdv.rdv_id
    JOIN {{ ref('heart_stg') }} hrt ON rhst.id_readmisson = hrt.id_heart  -- Vérification nécessaire
    -- Ajoutez ici d'autres jointures si nécessaire
)

SELECT
    id_urgent_visite,
    id_patient,
    id_hopital,
    date_visite,
    type_visite,
    duree_hospitalisation,
    specialite_medicale,
    diag_1,
    diag_2,
    diag_3,
    nb_urgences,
    nb_medicaments,
    admission_date,
    admission_type,
    time_stamp
FROM visite_urgence
