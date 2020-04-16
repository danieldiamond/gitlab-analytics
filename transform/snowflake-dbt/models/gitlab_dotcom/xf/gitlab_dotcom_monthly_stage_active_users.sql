WITH date_details AS (
  
    SELECT *
    FROM {{ ref('date_details') }}
  
)

, gitlab_dotcom_usage_data_events AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_usage_data_events') }}
  
)

, gitlab_subscriptions AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base') }}
)

, plans AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans') }}

)

SELECT 
  DATEADD('month', -1, date_day)                  AS smau_month,
  
  -- ids 
  gitlab_dotcom_usage_data_events.user_id,
  gitlab_dotcom_usage_data_events.namespace_id,
  
  
  -- user dimensions
  CASE
    WHEN gitlab_subscriptions.is_trial
      THEN 'trial'
    ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
  END                                                         AS plan_id_at_smau_month_end,
  CASE
    WHEN gitlab_subscriptions.is_trial
      THEN 'trial'
    ELSE COALESCE(plans.plan_name, 'free')
  END                                                         AS plan_name_at_smau_month_end,
  
  -- event data
  event_name,
  stage_name,
  is_representative_of_stage,
  
  --metadata
  DATEDIFF('day', user_created_at, date_day)      AS days_since_user_creation,
  DATEDIFF('day', namespace_created_at, date_day) AS days_since_namespace_creation,
  
  COUNT(*)                                        AS event_count,
  COUNT(DISTINCT TO_DATE(event_created_at))       AS event_day_count
FROM date_details
INNER JOIN gitlab_dotcom_usage_data_events 
  ON gitlab_dotcom_usage_data_events.event_created_at BETWEEN DATEADD('day', -28, date_details.date_day) AND date_day 
LEFT JOIN gitlab_subscriptions
  ON gitlab_dotcom_usage_data_events.namespace_id = gitlab_subscriptions.namespace_id
  AND DATEADD('day', -1, date_day) BETWEEN gitlab_subscriptions.valid_from
    AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
LEFT JOIN plans
  ON gitlab_subscriptions.plan_id = plans.plan_id
WHERE day_of_month = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10
