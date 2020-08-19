WITH project_statistics_snapshot_monthly AS (

  SELECT *
  FROM {{ ref("gitlab_dotcom_project_statistic_snapshots_monthly") }}

), namespace_lineage_monthly AS (

  SELECT *
  FROM {{ ref("gitlab_dotcom_namespace_lineage_monthly") }}

),  namespace_subscription_snapshot AS (

  SELECT
  *,
  IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
  FROM {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}

), joined AS (

  SELECT
    project_statistics_snapshot_monthly.snapshot_month                                                          AS month,
    project_statistics_snapshot_monthly.project_id                                                              AS project_id,
    project_statistics_snapshot_monthly.namespace_id                                                            AS namespace_id,
    namespace_lineage_monthly.ultimate_parent_id                                                                AS ultimate_parent_namespace_id,
    namespace_lineage_monthly.namespace_is_internal                                                             AS namespace_is_internal,
    namespace_subscription_snapshot.plan_id                                                                     AS plan_id,
    -- taking storage as of last updated time, so most of the time this will end up being the MAX value
    MAX(project_statistics_snapshot.storage_size/POW(1024,3))                                                   AS storage_size_GB,
    MAX(project_statistics_snapshot.repository_size/POW(1024,3))                                                AS repository_size_GB,
    MAX(project_statistics_snapshot.lfs_objects_size/POW(1024,3))                                               AS lfs_objects_size_GB,
    MAX(project_statistics_snapshot.build_artifacts_size/POW(1024,3))                                           AS build_artifacts_size_GB,
    MAX(project_statistics_snapshot.packages_size/POW(1024,3))                                                  AS packages_size_GB,
    MAX(project_statistics_snapshot.wiki_size/POW(1024,3))                                                      AS wiki_size_GB
    FROM project_statistics_snapshot_monthly
    INNER JOIN namespace_lineage_monthly
      ON project_statistics_snapshot_monthly.namespace_id = namespace_lineage_monthly.namespace_id
      AND project_statistics_snapshot_monthly.snapshot_month = namespace_lineage_monthly.snapshot_month
    INNER JOIN namespace_subscription_snapshot
      ON namespace_lineage_monthly.ultimate_parent_id = namespace_subscription_snapshot.namespace_id
      AND namespace_lineage_monthly.snapshot_month BETWEEN namespace_subscription_snapshot.valid_from AND namespace_subscription_snapshot.valid_to_
    GROUP BY 1,2,3,4,5,6
)

SELECT * FROM joined
