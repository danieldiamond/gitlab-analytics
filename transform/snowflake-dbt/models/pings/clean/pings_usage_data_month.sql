{{
  config(
    materialized='incremental',
    unique_key='unique_key'
  )
}}

WITH usage_data AS (
    SELECT * FROM {{ ref('pings_usage_data_unpacked') }}
    {% if is_incremental() %}
        WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE) - interval '1 month'
    {% endif %}
), usage_data_month_base AS (
    SELECT
      md5(usage_data.uuid || date_trunc('month', usage_data.created_at)::date)    AS unique_key,
      md5(usage_data.uuid || (date_trunc('month', usage_data.created_at) + INTERVAL '1 month') ::date) AS next_unique_key,
      uuid                                                                        AS uuid,
      DATE_TRUNC('month', created_at)                                             AS created_at,
      max(active_user_count)                                                      AS active_user_count,
      max(assignee_lists)                                                         AS assignee_lists,
      max(auto_devops_disabled)                                                   AS auto_devops_disabled,
      max(auto_devops_enabled)                                                    AS auto_devops_enabled,
      max(boards)                                                                 AS boards,
      max(ci_builds)                                                              AS ci_builds,
      max(ci_external_pipelines)                                                  AS ci_external_pipelines,
      max(ci_internal_pipelines)                                                  AS ci_internal_pipelines,
      max(ci_pipeline_config_auto_devops)                                         AS ci_pipeline_config_auto_devops,
      max(ci_pipeline_config_repository)                                          AS ci_pipeline_config_repository,
      max(ci_pipeline_schedules)                                                  AS ci_pipeline_schedules,
      max(ci_runners)                                                             AS ci_runners,
      max(ci_triggers)                                                            AS ci_triggers,
      max(clusters)                                                               AS clusters,
      max(clusters_applications_helm)                                             AS clusters_applications_helm,
      max(clusters_applications_ingress)                                          AS clusters_applications_ingress,
      max(clusters_applications_knative)                                          AS clusters_applications_knative,
      max(clusters_applications_prometheus)                                       AS clusters_applications_prometheus,
      max(clusters_applications_runner)                                           AS clusters_applications_runner,
      max(clusters_disabled)                                                      AS clusters_disabled,
      max(clusters_enabled)                                                       AS clusters_enabled,
      max(clusters_platforms_gke)                                                 AS clusters_platforms_gke,
      max(clusters_platforms_user)                                                AS clusters_platforms_user,
      max(container_scanning_jobs)                                                AS container_scanning_jobs,
      max(dast_jobs)                                                              AS dast_jobs,
      max(dependency_scanning_jobs)                                               AS dependency_scanning_jobs,
      max(deploy_keys)                                                            AS deploy_keys,
      max(deployments)                                                            AS deployments,
      max(edition)                                                                AS edition,
      max(main_edition)                                                           AS main_edition,
      max(edition_type)                                                           AS edition_type,
      max(environments)                                                           AS environments,
      max(epics)                                                                  AS epics,
      max(epics_deepest_relationship_level)                                       AS epics_deepest_relationship_level,      
      max(gcp_clusters)                                                           AS gcp_clusters,
      max(geo_nodes)                                                              AS geo_nodes,
      max(groups)                                                                 AS groups,
      max(id)                                                                     AS ping_id,
      max(in_review_folder)                                                       AS in_review_folder,
      max(issues)                                                                 AS issues,
      max(keys)                                                                   AS keys,
      max(label_lists)                                                            AS label_lists,
      max(labels)                                                                 AS labels,
      max(ldap_group_links)                                                       AS ldap_group_links,
      max(ldap_keys)                                                              AS ldap_keys,
      max(ldap_users)                                                             AS ldap_users,
      max(lfs_objects)                                                            AS lfs_objects,
      max(license_management_jobs)                                                AS license_management_jobs,
      max(merge_requests)                                                         AS merge_requests,
      max(milestone_lists)                                                        AS milestone_lists,
      max(milestones)                                                             AS milestones,
      max(notes)                                                                  AS notes,
      max(pages_domains)                                                          AS pages_domains,
      max(projects)                                                               AS projects,
      max(projects_imported_from_github)                                          AS projects_imported_from_github,
      max(projects_jira_active)                                                   AS projects_jira_active,
      max(projects_jira_cloud_active)                                             AS projects_jira_cloud_active,
      max(projects_jira_dvcs_cloud_active)                                        AS projects_jira_dvcs_cloud_active,
      max(projects_jira_dvcs_server_active)                                       AS projects_jira_dvcs_server_active,
      max(projects_jira_server_active)                                            AS projects_jira_server_active,
      max(projects_mirrored_with_pipelines_enabled)                               AS projects_mirrored_with_pipelines_enabled,
      max(projects_prometheus_active)                                             AS projects_prometheus_active,
      max(projects_reporting_ci_cd_back_to_github)                                AS projects_reporting_ci_cd_back_to_github,
      max(projects_slack_notifications_active)                                    AS projects_slack_notifications_active,
      max(projects_slack_slash_active)                                            AS projects_slack_slash_active,
      max(protected_branches)                                                     AS protected_branches,
      max(releases)                                                               AS releases,
      max(remote_mirrors)                                                         AS remote_mirrors,
      max(sast_jobs)                                                              AS sast_jobs,
      max(service_desk_enabled_projects)                                          AS service_desk_enabled_projects,
      max(service_desk_issues)                                                    AS service_desk_issues,
      max(snippets)                                                               AS snippets,
      max(todos)                                                                  AS todos,
      max(uploads)                                                                AS uploads,
      max(web_hooks)                                                              AS web_hooks
    FROM usage_data
    GROUP BY 1, 2, 3, 4
)

SELECT
  this_month.*,
  CASE WHEN next_month.next_unique_key IS NOT NULL
          THEN FALSE
       ELSE TRUE
  END AS churned_next_month,
  CASE WHEN this_month.uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
          THEN 'SaaS'
       ELSE 'Self-Hosted'
  END AS ping_source
FROM usage_data_month_base this_month
LEFT JOIN usage_data_month_base next_month
  ON this_month.next_unique_key = next_month.unique_key
