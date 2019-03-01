{{ config(schema='analytics') }}

with users as (SELECT
  *,
  TIMESTAMPDIFF(DAYS, user_created_at, last_activity_on)                                      as days_active,
  TIMESTAMPDIFF(DAYS, user_created_at, CURRENT_TIMESTAMP(2))                                  as account_age,
  CASE
    WHEN account_age <= 1 THEN '1 - 1 day or less'
    WHEN account_age <= 7 THEN '2 - 2 to 7 days'
    WHEN account_age <= 14 THEN '3 - 8 to 14 days'
    WHEN account_age <= 30 THEN '4 - 15 to 30 days'
    WHEN account_age <= 60 THEN '5 - 31 to 60 days'
    WHEN account_age > 60 THEN '6 - Over 60 days'
  END                                                                                         as account_age_cohort

FROM {{ref('gitlab_dotcom_users')}}

)

SELECT *
FROM users