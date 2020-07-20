WITH date_details AS (
  
    SELECT *
    FROM {{ ref("date_details") }}
    WHERE date_day >= '2019-02-01'::DATE
     
), project_snapshots AS (

   SELECT
     *,
     IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
   FROM {{ ref('gitlab_dotcom_project_statistics_snapshots_base') }}

), project_snapshots_history AS (
  
    SELECT
      date_details.date_actual                      AS date_actual,
      DATE_TRUNC('month', date_details.date_actual) AS snapshot_month,
      project_snapshots.project_statistics_id,
      project_snapshots.project_id,
      project_snapshots.namespace_id,
      project_snapshots.commit_count,
      project_snapshots.storage_size,
      project_snapshots.repository_size,
      project_snapshots.lfs_objects_size,
      project_snapshots.build_artifacts_size,
      project_snapshots.shared_runners_seconds,
      project_snapshots.last_update_started_at
    FROM project_snapshots
    INNER JOIN date_details
      ON date_details.date_actual BETWEEN project_snapshots.valid_from AND project_snapshots.valid_to_
  
), project_snapshots_monthly AS (
    
    SELECT
      snapshot_month,
      project_statistics_id,
      project_id,
      namespace_id,
      commit_count,
      storage_size,
      repository_size,
      lfs_objects_size,
      build_artifacts_size,
      shared_runners_seconds,
      last_update_started_at
    FROM project_snapshots_history
    QUALIFY ROW_NUMBER() OVER(PARTITION BY snapshot_month, project_id ORDER BY date_actual DESC) = 1
  
)

SELECT *
FROM project_snapshots_monthly
