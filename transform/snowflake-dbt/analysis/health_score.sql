WITH subscription_periods AS (
  
  SELECT *
  FROM analytics.zuora_subscription_periods
  WHERE subscription_version_term_end_date BETWEEN '2017-01-01' AND CURRENT_DATE
  
),

customers AS (
  
  SELECT *
  FROM analytics_staging.customers_db_orders
  WHERE gitlab_namespace_id IS NOT NULL
    AND subscription_name_slugify IS NOT NULL

),

joined AS (

  SELECT
    subscription_periods.*,
    customers.gitlab_namespace_id
  FROM subscription_periods
    LEFT JOIN customers
      ON subscription_periods.subscription_name_slugify = customers.subscription_name_slugify
  WHERE latest_delivery = 'SaaS'
    AND customers.gitlab_namespace_id = '4273364' -- TEMP

),

/* Start Usage Data Here */
-- Missing: add the `events` tables

with_events AS (
  
  SELECT
    joined.subscription_id,
    IFF(joined.is_renewed = TRUE, 1, 0) AS is_renewed,
    events.user_id,
    events.event_name
  FROM joined
    LEFT JOIN analytics.gitlab_dotcom_usage_data_events AS events
      ON joined.gitlab_namespace_id = events.namespace_id
      AND events.event_created_at BETWEEN DATEADD(days, -30, joined.subscription_version_term_end_date) AND joined.subscription_version_term_end_date
)


SELECT * 
FROM with_events
 PIVOT(COUNT(user_id) FOR event_name IN ('boards', 'ci_builds', 'ci_pipeline_schedules', 'ci_pipelines', 'ci_stages', 'ci_triggers', 'clusters_applications_helm', 'deployments', 'environments', 'epics', 'groups', 'issues', 'labels', 'lfs_objects', 'merge_requests', 'milestones', 'notes', 'project_auto_devops', 'projects_prometheus_active', 'releases', 'snippets', 'todos'))
;





