WITH date_details AS (
  
    SELECT *
    FROM {{ref("date_details")}}
    WHERE date_day >= '2019-02-01'::DATE
     
), namespace_statistics_snapshots AS (

   SELECT
     *,
     IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
   FROM {{ ref('gitlab_dotcom_namespace_root_storage_statistics_snapshots_base') }}

), namespace_statistics_snapshots_history AS (
  
    SELECT
      date_details.date_actual                      AS date_actual,
      DATE_TRUNC('month', date_details.date_actual) AS snapshot_month,
      namespace_statistics_snapshots.namespace_id,
      namespace_statistics_snapshots.repository_size,
      namespace_statistics_snapshots.lfs_objects_size,
      namespace_statistics_snapshots.wiki_size,
      namespace_statistics_snapshots.build_artifacts_size,
      namespace_statistics_snapshots.storage_size,
      namespace_statistics_snapshots.packages_size
    FROM namespace_statistics_snapshots
    INNER JOIN date_details
      ON date_details.date_actual BETWEEN namespace_statistics_snapshots.valid_from AND namespace_statistics_snapshots.valid_to_
  
), namespace_statistics_snapshots_monthly AS (
    
    SELECT
      snapshot_month,
      namespace_id,
      repository_size,
      lfs_objects_size,
      wiki_size,
      build_artifacts_size,
      storage_size,
      packages_size
    FROM namespace_statistics_snapshots_history
    QUALIFY ROW_NUMBER() OVER(PARTITION BY snapshot_month, namespace_id ORDER BY date_actual DESC) = 1
  
)

SELECT *
FROM namespace_statistics_snapshots_monthly
