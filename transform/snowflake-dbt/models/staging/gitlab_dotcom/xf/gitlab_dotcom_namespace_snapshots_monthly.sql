WITH date_details AS (
  
    SELECT *
    FROM {{ ref("date_details") }}
    WHERE date_day >= '2019-02-01'::DATE
    QUALIFY ROW_NUMBER() OVER(PARTITION BY first_day_of_month ORDER BY date_actual DESC) = 1
     
), namespace_snapshots AS (
   SELECT
     *,
     IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
   FROM {{ ref('gitlab_dotcom_namespaces_snapshots_base') }}
  -- where namespace_id = 8239636
), namespace_snapshots_history AS (
  
    SELECT
      date_details.date_actual                      AS date_actual,
      DATE_TRUNC('month', date_details.date_actual) AS snapshot_month,
      namespace_snapshots.namespace_id,
      namespace_snapshots.plan_id,
      namespace_snapshots.parent_id,
      namespace_snapshots.owner_id,
      namespace_snapshots.namespace_type,
      namespace_snapshots.visibility_level,
      namespace_snapshots.shared_runners_minutes_limit,
      namespace_snapshots.extra_shared_runners_minutes_limit
    FROM namespace_snapshots
    INNER JOIN date_details
      ON date_details.date_actual BETWEEN namespace_snapshots.valid_from AND namespace_snapshots.valid_to_
  
), namespace_snapshots_monthly AS (
    
    SELECT
      snapshot_month,
      namespace_id,
      plan_id,
      parent_id,
      owner_id,
      namespace_type,
      visibility_level,
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit
    FROM namespace_snapshots_history
    QUALIFY ROW_NUMBER() OVER(PARTITION BY snapshot_month, namespace_id ORDER BY date_actual DESC) = 1
  
)

SELECT *
FROM namespace_snapshots_monthly
