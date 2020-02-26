WITH usage_data_events AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_usage_data_events')}}
    WHERE event_created_at IS NOT NULL --TODO: add test earlier
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
      DATE_TRUNC('day', date_actual)::DATE    AS event_day,
      DATE_TRUNC('week', date_actual)::DATE   AS event_week,
      DATE_TRUNC('month', date_actual)::DATE  AS event_month,
      event_name,
      stage_name,
      plan_name_at_event_date,
    FROM date_details
      INNER JOIN spine_cols
        ON 1=1
      
)



final AS (
  SELECT DISTINCT
    spine.event_day,
    spine.event_week,
    spine.event_month,
    spine.event_name,
    spine.stage_name,
    spine.plan_name_at_event_date,

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
  FROM spine
    LEFT JOIN usage_data_events
      ON spine.event_day = DATE_TUNC('day', usage_data_events.event_created_at)
      AND spine.event_name = usage_data_events.event_name
      AND spine.stage_name = usage_data_events.stage_name
      AND spine.plan_name_at_event_date = usage_data_events.plan_name_at_event_date
)

SELECT *
FROM agg
ORDER BY
  event_day,
  stage_name,
  event_name