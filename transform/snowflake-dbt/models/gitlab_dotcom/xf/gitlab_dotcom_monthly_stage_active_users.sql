{{
  config({
    "materialized": "incremental"
  })
}}


WITH date_details AS (
  
    SELECT *
    FROM {{ ref('date_details') }}
  
)

, gitlab_dotcom_usage_data_events AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_usage_data_events') }}
  
)

SELECT 
  DATEADD('month', -1, date_day)            AS smau_month,
  user_id,
  namespace_id,
  event_name,
  stage_name,
  COUNT(*)                                  AS event_count,
  COuNT(DISTINCT TO_DATE(event_created_at)) AS event_day_count
FROM date_details
LEFT JOIN gitlab_dotcom_usage_data_events ON gitlab_dotcom_usage_data_events.event_created_at BETWEEN DATEADD('day', -28, date_details.date_day) AND date_day 
WHERE day_of_month = 1 AND date_day < current_date
GROUP BY 1,2,3,4,5
