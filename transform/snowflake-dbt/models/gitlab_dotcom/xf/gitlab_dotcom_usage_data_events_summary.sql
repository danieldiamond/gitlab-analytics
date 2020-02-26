WITH usage_data_events AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_usage_data_events')}}
    LIMIT 10000 --TODO

),

date_details AS (

    SELECT DISTINCT
      date_actual
    FROM {{ref('date_details')}}
    WHERE date_actual BETWEEN '2013-01-01' AND CURRENT_DATE

),

spine_cols AS (

    SELECT DISTINCT
      event_name,
      stage_name,
      plan_name_at_event_date,
    FROM usage_data_events

),

spine AS (
    
    SELECT DISTINCT
      date_actual,
      event_name,
      stage_name,
      plan_name_at_event_date,
    FROM date_details
      INNER JOIN spine_cols
        ON 1=1
      
)



final AS (
  SELECT DISTINCT
    DATE_TRUNC('day', event_created_at)::DATE   AS event_day,
    DATE_TRUNC('week', event_created_at)::DATE  AS event_week,
    DATE_TRUNC('month', event_created_at)::DATE AS event_month,
    event_name,
    stage_name,
    plan_name_at_event_date,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_day, event_name)   AS unique_users_by_event_and_day,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_week, event_name)  AS unique_users_by_event_and_week,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_month, event_name) AS unique_users_by_event_and_month,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_day, stage_name)   AS unique_users_by_stage_and_day,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_week, stage_name)  AS unique_users_by_stage_and_week,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_month, stage_name) AS unique_users_by_stage_and_month,

    COUNT(DISTINCT user_id) OVER (PARTITION BY event_day, event_name, plan_name_at_event_date)   AS unique_users_by_event_and_day_and_plan,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_week, event_name, plan_name_at_event_date)  AS unique_users_by_event_and_week_and_plan,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_month, event_name, plan_name_at_event_date) AS unique_users_by_event_and_month_and_plan,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_day, stage_name, plan_name_at_event_date)   AS unique_users_by_stage_and_day_and_plan,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_week, stage_name, plan_name_at_event_date)  AS unique_users_by_stage_and_week_and_plan,
    COUNT(DISTINCT user_id) OVER (PARTITION BY event_month, stage_name, plan_name_at_event_date) AS unique_users_by_stage_and_month_and_plan
  FROM usage_data_events
  WHERE event_created_at IS NOT NULL
)

SELECT *
FROM agg
ORDER BY
  event_day,
  stage_name,
  event_name