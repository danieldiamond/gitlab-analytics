{{ config({
    "schema": "staging"
    })
}}

WITH users AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_users') }}

), cohorting AS (

  SELECT user_id,
         user_created_at::DATE                                        AS cohort_date,
         TIMESTAMPDIFF(MONTHS,user_created_at,last_activity_on)         AS period
  FROM users

), joined AS (

  SELECT DATE_TRUNC('month', cohorting.cohort_date)                     AS cohort_date,
         cohorting.period,
         COUNT(DISTINCT cohorting.user_id)                              AS active_in_period_distinct_count,
         COUNT(DISTINCT base_cohort.user_id)                            AS base_cohort_count,
         active_in_period_distinct_count/base_cohort_count :: FLOAT     AS retention

  FROM cohorting
  JOIN cohorting AS base_cohort
    ON cohorting.cohort_date = base_cohort.cohort_date
  AND base_cohort.period = 0
  WHERE cohorting.period IS NOT NULL
    AND cohorting.period >= 0
  GROUP BY 1, 2
  ORDER BY cohort_date DESC

)

SELECT
  md5(cohort_date || period)                                       AS cohort_key,
  *
FROM joined