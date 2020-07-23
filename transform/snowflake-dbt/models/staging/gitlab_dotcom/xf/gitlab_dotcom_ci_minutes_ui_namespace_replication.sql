WITH project_snapshot_monthly AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_project_snapshots_monthly') }}
    WHERE snapshot_month >= '2020-07-01'

), namespace_lineage_monthly AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_lineage_monthly') }}
    WHERE snapshot_month >= '2020-07-01'

), namespace_statistic_monthly AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_statistics_snapshots_monthly') }}
    WHERE snapshot_month >= '2020-07-01'

), namespace_lineage_current AS (

    SELECT
      DATE_TRUNC('month', CURRENT_DATE) AS snapshot_month,
      namespace_id,
      parent_id,
      upstream_lineage,
      ultimate_parent_id,
      namespace_is_internal
    FROM {{ ref('gitlab_dotcom_namespace_lineage') }}

), namespace_snapshots_monthly AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_snapshots_monthly') }}
    WHERE snapshot_month >= '2020-07-01'

), namespace_statistic_current AS (

    SELECT
      DATE_TRUNC('month', CURRENT_DATE) AS snapshot_month,
      namespace_id,
      shared_runners_seconds,
      shared_runners_seconds_last_reset
    FROM {{ ref('gitlab_dotcom_namespace_statistics') }}

), namespace_current AS (

    SELECT
      DATE_TRUNC('month', CURRENT_DATE) AS snapshot_month,
      namespace_id,
      NULLIF(plan_id, 'trial')          AS plan_id,
      parent_id,
      owner_id,
      namespace_type,
      visibility_level,
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit
    FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

), project_current AS (

    SELECT
      DATE_TRUNC('month', CURRENT_DATE) AS snapshot_month,
      project_id,
      namespace_id,
      visibility_level,
      shared_runners_enabled
    FROM {{ ref('gitlab_dotcom_projects_xf') }}

), project_snapshot_monthly_all AS (

    SELECT *
    FROM project_snapshot_monthly
  
    UNION
  
    SELECT *
    FROM project_current

), namespace_lineage_monthly_all AS (

    SELECT *
    FROM namespace_lineage_monthly

    UNION
  
    SELECT *
    FROM namespace_lineage_current
  
), namespace_snapshots_monthly_all AS (

    SELECT *
    FROM namespace_snapshots_monthly
  
    UNION
  
    SELECT *
    FROM namespace_current
      
), namespace_statistic_monthly_all AS (
  
    SELECT *
    FROM namespace_statistic_monthly
  
    UNION
  
    SELECT *
    FROM namespace_statistic_current
  
), child_projects_enabled_shared_runners_any AS (

    SELECT
      project_snapshot_monthly_all.snapshot_month,
      ultimate_parent_id,
      SUM(IFF(shared_runners_enabled, 1, 0)) AS shared_runners_enabled_int,
      COUNT(1) AS project_count
    FROM project_snapshot_monthly_all
    INNER JOIN namespace_lineage_monthly_all
      ON namespace_lineage_monthly_all.namespace_id = project_snapshot_monthly_all.namespace_id
      AND namespace_lineage_monthly_all.snapshot_month = project_snapshot_monthly_all.snapshot_month
    GROUP BY 1, 2
    
), namespace_statistic_monthly_top_level AS (

    SELECT namespace_statistic_monthly_all.*
    FROM namespace_statistic_monthly_all
    INNER JOIN namespace_snapshots_monthly_all
      ON namespace_statistic_monthly_all.namespace_id = namespace_snapshots_monthly_all.namespace_id
      AND namespace_statistic_monthly_all.snapshot_month = namespace_snapshots_monthly_all.snapshot_month
      AND namespace_snapshots_monthly_all.parent_id IS NULL  -- Only top level namespaces
      
), ci_minutes_logic AS (
    
    SELECT
      namespace_statistic_monthly_top_level.snapshot_month,
      namespace_statistic_monthly_top_level.namespace_id,
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit,
      2000                                          AS gitlab_current_settings_shared_runners_minutes,
      child_projects_enabled_shared_runners_any.shared_runners_enabled_int,
      project_count,
      namespace_statistic_monthly_top_level.shared_runners_seconds
                                                    AS shared_runners_seconds_used,
      shared_runners_seconds_used / 60              AS shared_runners_minutes_used,
      IFF(namespace_snapshots_monthly_all.parent_id IS NULL, True, False)
                                                    AS has_parent_not,
      IFF(has_parent_not, True, False)              AS shared_runners_minutes_supported,
      IFF(child_projects_enabled_shared_runners_any.shared_runners_enabled_int > 0,
          True, False)                              AS shared_runners_enabled,
      IFF(
          IFF(shared_runners_minutes_limit IS NOT NULL,
          shared_runners_minutes_limit + IFNULL(extra_shared_runners_minutes_limit, 0),
          gitlab_current_settings_shared_runners_minutes + IFNULL(extra_shared_runners_minutes_limit, 0)
          ) > 0,
          True, False)                              AS actual_shared_runners_minutes_limit_non_zero,    
      IFF(shared_runners_minutes_supported AND shared_runners_enabled AND actual_shared_runners_minutes_limit_non_zero,
          True, False)                              AS shared_runners_minutes_limit_enabled,   
      shared_runners_minutes_used                   AS minutes_used,
      IFNULL(shared_runners_minutes_limit, gitlab_current_settings_shared_runners_minutes)
                                                    AS monthly_minutes, 
      IFNULL(extra_shared_runners_minutes_limit, 0) AS purchased_minutes,
      IFF(purchased_minutes = 0, True, False)       AS no_minutes_purchased,
          IFF(minutes_used <= monthly_minutes, True, False)
                                                    AS monthly_minutes_available,
      IFF(no_minutes_purchased OR monthly_minutes_available, 0, minutes_used - monthly_minutes)
                                                    AS purchased_minutes_used,
      minutes_used - purchased_minutes_used         AS monthly_minutes_used,
      IFF(shared_runners_minutes_limit_enabled AND monthly_minutes_used >= monthly_minutes, True, False)
                                                    AS monthly_minutes_used_up,
      IFF(purchased_minutes > 0, True, False)       AS any_minutes_purchased,
      IFF(shared_runners_minutes_limit_enabled AND any_minutes_purchased AND purchased_minutes_used >= purchased_minutes,
          True, False)                              AS purchased_minutes_used_up,    
      monthly_minutes_used                          AS used,
      IFF(shared_runners_minutes_limit_enabled, monthly_minutes::VARCHAR, 'Unlimited')
                                                    AS limit,
      monthly_minutes::VARCHAR                      AS limit_based_plan,
      CASE
          WHEN shared_runners_minutes_limit_enabled
          THEN IFF(monthly_minutes_used_up, 'Over Quota', 'Under Quota')
          ELSE 'Disabled'
      END                                           AS status,
      IFF(monthly_minutes_used >= monthly_minutes, 'Over Quota', 'Under Quota')
                                                    AS status_based_plan,
      purchased_minutes_used                        AS used_purchased,
      purchased_minutes                             AS limit_purchased,
      IFF(purchased_minutes_used_up, 'Over Quota', 'Under Quota')
                                                    AS status_purchased
          
    FROM namespace_statistic_monthly_top_level
    INNER JOIN namespace_snapshots_monthly_all
      ON namespace_snapshots_monthly_all.snapshot_month = namespace_statistic_monthly_top_level.snapshot_month
      AND namespace_snapshots_monthly_all.namespace_id = namespace_statistic_monthly_top_level.namespace_id
    LEFT JOIN child_projects_enabled_shared_runners_any
      ON child_projects_enabled_shared_runners_any.snapshot_month = namespace_statistic_monthly_top_level.snapshot_month
      AND child_projects_enabled_shared_runners_any.ultimate_parent_id = namespace_statistic_monthly_top_level.namespace_id

)

SELECT
  snapshot_month,
  namespace_id,
  shared_runners_minutes_used AS shared_runners_minutes_used_overall,
  used,
  limit,
  limit_based_plan,
  status,
  used_purchased,
  limit_purchased,
  status_purchased
FROM ci_minutes_logic
