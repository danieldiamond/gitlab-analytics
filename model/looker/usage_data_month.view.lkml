view: usage_data_month {
  derived_table: {
    sql:
WITH usage_count_month AS (
SELECT
  ud.uuid AS uuid,
  DATE_TRUNC('month', ud.created_at)                            AS created_at,
  MAX(ud.edition) AS edition,
  MAX((ud.stats->'auto_devops_enabled')::text::int)             AS auto_devops_enabled,
  MAX((ud.stats->'boards')::text::int)                          AS boards,
  MAX((ud.stats->'ci_builds')::text::int)                       AS ci_builds,
  MAX((ud.stats->'deployments')::text::int)                     AS deployments,
  MAX((ud.stats->'environments')::text::int)                    AS environments,
  MAX((ud.stats->'gcp_clusters')::text::int)                    AS gcp_clusters,
  MAX((ud.stats->'groups')::text::int)                          AS groups,
  MAX((ud.stats->'issues')::text::int)                          AS issues,
  MAX((ud.stats->'lfs_objects')::text::int)                     AS lfs_objects,
  MAX((ud.stats->'merge_requests')::text::int)                  AS merge_requests,
  MAX((ud.stats->'milestones')::text::int)                      AS milestones,
  MAX((ud.stats->'projects')::text::int)                        AS projects,
  MAX((ud.stats->'projects_prometheus_active')::text::int)      AS projects_prometheus_active
FROM version.usage_data ud
WHERE ud.created_at > (CURRENT_DATE - INTERVAL '420 days')
AND ud.created_at < date_trunc('month', CURRENT_DATE)
GROUP BY 1, 2
)

SELECT
  *,
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
    ;;
  }

  dimension: created_at_month {
    type: date_month
    sql: ${TABLE}.created_at ;;
  }

  dimension: edition {
    type: string
    sql: ${TABLE}.edition ;;
  }

  # UUID

  dimension: uuid {
    type: string
    sql: ${TABLE}.uuid ;;
  }

  measure: distinct_uuid_count {
    type: count_distinct
    sql: ${uuid} ;;
  }

  # Usage metrics

  dimension: auto_devops_count {
    type: number
    sql: ${TABLE}.auto_devops_enabled ;;
  }

  measure: auto_devops_instance_count {
    type: sum
    sql: ${TABLE}.auto_devops_enabled ;;
  }

  dimension: boards_count {
    type: number
    sql: ${TABLE}.boards ;;
  }

  measure: boards_instance_count {
    type: sum
    sql: ${TABLE}.boards_active ;;
  }

  dimension: ci_builds_count {
    type: number
    sql: ${TABLE}.ci_builds ;;
  }

  measure: ci_builds_instance_count {
    type: sum
    sql: ${TABLE}.ci_builds_active ;;
  }

  dimension: deployments_count {
    type: number
    sql: ${TABLE}.deployments ;;
  }

  measure: deployments_instance_count {
    type: sum
    sql: ${TABLE}.deployments_active ;;
  }

  dimension: environments_count {
    type: number
    sql: ${TABLE}.environments ;;
  }

  measure: environments_instance_count {
    type: sum
    sql: ${TABLE}.environments_active ;;
  }

  dimension: gcp_clusters_count {
    type: number
    sql: ${TABLE}.gcp_clusters ;;
  }

  measure: gcp_clusters_instance_count {
    type: sum
    sql: ${TABLE}.gcp_clusters_active ;;
  }

  dimension: groups_count {
    type: number
    sql: ${TABLE}.groups ;;
  }

  measure: groups_instance_count {
    type: sum
    sql: ${TABLE}.groups_active ;;
  }

  dimension: issues_count {
    type: number
    sql: ${TABLE}.issues ;;
  }

  measure: issues_instance_count {
    type: sum
    sql: ${TABLE}.issues_active ;;
  }

  dimension: lfs_objects_count {
    type: number
    sql: ${TABLE}.lfs_objects ;;
  }

  measure: lfs_objects_instance_count {
    type: sum
    sql: ${TABLE}.lfs_objects_active ;;
  }

  dimension: merge_requests_count {
    type: number
    sql: ${TABLE}.merge_requests ;;
  }

  measure: merge_requests_instance_count {
    type: sum
    sql: ${TABLE}.merge_requests_active ;;
  }

  dimension: milestones_count {
    type: number
    sql: ${TABLE}.milestones ;;
  }

  measure: milestones_instance_count {
    type: sum
    sql: ${TABLE}.milestones_active ;;
  }

  dimension: projects_count {
    type: number
    sql: ${TABLE}.projects ;;
  }

  measure: projects_instance_count {
    type: sum
    sql: ${TABLE}.projects_active ;;
  }

  dimension: projects_prometheus_active_count {
    type: number
    sql: ${TABLE}.projects_prometheus_active ;;
  }

  measure: projects_prometheus_active_instance_count {
    type: sum
    sql: ${TABLE}.projects_prometheus_active_active ;;
  }

}
