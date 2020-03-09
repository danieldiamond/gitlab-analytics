{{ config({
    "schema": "sensitive"
    })
}}
WITH mailing_list_distinct_versions AS (
    
    SELECT DISTINCT 
      mailing_list_id,
      mailing_list_membership_observed_at
    FROM {{ ref('qualtrics_mailing_contacts') }}

), distribution_mailing_list_version AS (

    SELECT
      dist.mailing_list_id AS mailing_list_id,
      dist.distribution_id AS distribution_id,
      dist.survey_id       AS survey_id,
      dist.mailing_sent_at AS mailing_sent_at,
      min(mailing_list_membership_observed_at) AS mailing_list_membership_observed_at
    FROM {{ ref('qualtrics_distribution') }} dist
    INNER JOIN mailing_list_distinct_versions ml
      ON dist.send_date < ml.mailing_list_membership_observed_at
    {{ dbt_utils.group_by(n=4) }}

), distribution_contacts_joined AS (

    SELECT 
        m.contact_email,
        d.mailing_sent_at,
        s.survey_name
    FROM distribution_mailing_list_version d
    INNER JOIN {{ ref('qualtrics_mailing_contacts') }} m
      ON d.mailing_list_membership_observed_at = m.mailing_list_membership_observed_at AND NOT m.is_unsubscribed
    INNER JOIN {{ ref('qualtrics_survey') }} s
      ON d.survey_id = s.survey_id

)
SELECT *
FROM distribution_contacts_joined