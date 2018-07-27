WITH usage_data AS (
    SELECT * FROM {{ ref('pings_usage_data_unpacked') }}
),

usage_count_month AS (
    SELECT
      uuid AS uuid,
      DATE_TRUNC('month', created_at)      AS created_at,
      MAX(edition)                         AS edition,
      MAX(auto_devops_enabled)             AS auto_devops_enabled,
      MAX(boards)                          AS boards,
      MAX(ci_builds)                       AS ci_builds,
      MAX(deployments)                     AS deployments,
      MAX(environments)                    AS environments,
      MAX(gcp_clusters)                    AS gcp_clusters,
      MAX(groups)                          AS groups,
      MAX(issues)                          AS issues,
      MAX(lfs_objects)                     AS lfs_objects,
      MAX(merge_requests)                  AS merge_requests,
      MAX(milestones)                      AS milestones,
      MAX(projects)                        AS projects,
      MAX(projects_prometheus_active)      AS projects_prometheus_active
    FROM usage_data
    WHERE created_at > (CURRENT_DATE - INTERVAL '420 days')
    AND created_at < date_trunc('month', CURRENT_DATE)
    GROUP BY 1, 2
)



SELECT
  uuid,
  created_at,
  edition,
  (auto_devops_enabled > 0)::int        AS auto_devops_enabled_active,
  (boards > 0)::int                     AS boards_active,
  (ci_builds > 0)::int                  AS ci_builds_active,
  (deployments > 0)::int                AS deployments_active,
  (environments > 0)::int               AS environments_active,
  (gcp_clusters > 0)::int               AS gcp_clusters_active,
  (groups > 0)::int                     AS groups_active,
  (issues > 0)::int                     AS issues_active,
  (lfs_objects > 0)::int                AS lfs_objects_active,
  (merge_requests > 0)::int             AS merge_requests_active,
  (milestones > 0)::int                 AS milestones_active,
  (projects > 0)::int                   AS projects_active,
  (projects_prometheus_active > 0)::int AS projects_prometheus_active_active
FROM usage_count_month

