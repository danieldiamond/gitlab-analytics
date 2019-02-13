{{
  config(
    materialized='incremental',
    unique_key='id'
  )
}}

WITH usage_data AS (
    SELECT * FROM {{ ref('pings_usage_data') }}
)

SELECT
  id,
  source_ip,
  version,
  installation_type,
  active_user_count,
  created_at,
  mattermost_enabled,
  uuid,
  edition,
  CASE
    WHEN version LIKE '%ee%'
      THEN 'EE'
    ELSE 'CE' END                                                   AS main_edition,
  CASE
--     Comes from https://gitlab.com/gitlab-org/gitlab-ee/blob/2dae25c3b780205f072833cd290e481dae436f3b/lib/gitlab/usage_data.rb#L154
    WHEN edition LIKE '%CE%'
      THEN 'Core'
    WHEN edition LIKE '%EES%'
      THEN 'Starter'
    WHEN edition LIKE '%EEP%'
      THEN 'Premium'
    WHEN edition LIKE '%EEU%'
      THEN 'Ultimate'
    WHEN edition LIKE '%EE Free%'
      THEN 'Core'
    WHEN edition LIKE '%EE%'
      THEN 'Starter'
    ELSE null END             AS edition_type,
  concat(concat(SPLIT_PART(version, '.', 1), '.'), SPLIT_PART(version, '.', 2)) AS major_version,
  hostname,
  host_id,
  (stats_used['assignee_lists']::numeric) AS assignee_lists,
  (stats_used['auto_devops_disabled']::numeric)                      AS auto_devops_disabled,
  (stats_used['auto_devops_enabled']::numeric)                       AS auto_devops_enabled,
  (stats_used['boards']::numeric)                                    AS boards,
  (stats_used['ci_builds']::numeric)                                 AS ci_builds,
  (stats_used['ci_external_pipelines']::numeric)                     AS ci_external_pipelines,
  (stats_used['ci_internal_pipelines']::numeric)                     AS ci_internal_pipelines,
  (stats_used['ci_pipeline_config_auto_devops']::numeric)            AS ci_pipeline_config_auto_devops,
  (stats_used['ci_pipeline_config_repository']::numeric)             AS ci_pipeline_config_repository,
  (stats_used['ci_pipeline_schedules']::numeric)                     AS ci_pipeline_schedules,
  (stats_used['ci_runners']::numeric)                                AS ci_runners,
  (stats_used['ci_triggers']::numeric)                               AS ci_triggers,
  (stats_used['clusters']::numeric)                                  AS clusters,
  (stats_used['clusters_applications_helm']::numeric)                AS clusters_applications_helm,
  (stats_used['clusters_applications_ingress']::numeric)             AS clusters_applications_ingress,
  (stats_used['clusters_applications_knative']::numeric)             AS clusters_applications_knative,
  (stats_used['clusters_applications_prometheus']::numeric)          AS clusters_applications_prometheus,
  (stats_used['clusters_applications_runner']::numeric)              AS clusters_applications_runner,
  (stats_used['clusters_disabled']::numeric)                         AS clusters_disabled,
  (stats_used['clusters_enabled']::numeric)                          AS clusters_enabled,
  (stats_used['clusters_platforms_gke']::numeric)                    AS clusters_platforms_gke,
  (stats_used['clusters_platforms_user']::numeric)                   AS clusters_platforms_user,
  (stats_used['container_scanning_jobs']::numeric)                   AS container_scanning_jobs,
  (stats_used['dast_jobs']::numeric)                                 AS dast_jobs,
  (stats_used['dependency_scanning_jobs']::numeric)                  AS dependency_scanning_jobs,
  (stats_used['deploy_keys']::numeric)                               AS deploy_keys,
  (stats_used['deployments']::numeric)                               AS deployments,
  (stats_used['environments']::numeric)                              AS environments,
  (stats_used['epics']::numeric)                                     AS epics,
  (stats_used['epics_deepest_relationship_level']::numeric)          AS epics_deepest_relationship_level,  
  (stats_used['gcp_clusters']::numeric)                              AS gcp_clusters,
  (stats_used['geo_nodes']::numeric)                                 AS geo_nodes,
  (stats_used['groups']::numeric)                                    AS groups,
  (stats_used['in_review_folder']::numeric)                          AS in_review_folder,
  (stats_used['issues']::numeric)                                    AS issues,
  (stats_used['keys']::numeric)                                      AS keys,
  (stats_used['labels']::numeric)                                    AS labels,
  (stats_used['label_lists']::numeric)                               AS label_lists,
  (stats_used['ldap_group_links']::numeric)                          AS ldap_group_links,
  (stats_used['ldap_keys']::numeric)                                 AS ldap_keys,
  (stats_used['ldap_users']::numeric)                                AS ldap_users,
  (stats_used['lfs_objects']::numeric)                               AS lfs_objects,
  (stats_used['license_management_jobs']::numeric)                   AS license_management_jobs,
  (stats_used['merge_requests']::numeric)                            AS merge_requests,
  (stats_used['milestones']::numeric)                                AS milestones,
  (stats_used['milestone_lists']::numeric)                           AS milestone_lists,
  (stats_used['notes']::numeric)                                     AS notes,
  (stats_used['pages_domains']::numeric)                             AS pages_domains,
  (stats_used['projects_prometheus_active']::numeric)                AS projects_prometheus_active,
  (stats_used['projects']::numeric)                                  AS projects,
  (stats_used['projects_imported_from_github']::numeric)             AS projects_imported_from_github,
  (stats_used['projects_jira_active']::numeric)                      AS projects_jira_active,
  (stats_used['projects_jira_cloud_active']::numeric)                AS projects_jira_cloud_active,
  (stats_used['projects_jira_dvcs_cloud_active']::numeric)           AS projects_jira_dvcs_cloud_active,
  (stats_used['projects_jira_dvcs_server_active']::numeric)          AS projects_jira_dvcs_server_active,
  (stats_used['projects_jira_server_active']::numeric)               AS projects_jira_server_active,
  (stats_used['projects_mirrored_with_pipelines_enabled']::numeric)  AS projects_mirrored_with_pipelines_enabled,
  (stats_used['projects_reporting_ci_cd_back_to_github']::numeric)   AS projects_reporting_ci_cd_back_to_github,
  (stats_used['projects_slack_notifications_active']::numeric)       AS projects_slack_notifications_active,
  (stats_used['projects_slack_slash_active']::numeric)               AS projects_slack_slash_active  ,
  (stats_used['protected_branches']::numeric)                        AS protected_branches,
  (stats_used['releases']::numeric)                                  AS releases,
  (stats_used['remote_mirrors']::numeric)                            AS remote_mirrors,
  (stats_used['sast_jobs']::numeric)                                 AS sast_jobs,
  (stats_used['service_desk_enabled_projects']::numeric)             AS service_desk_enabled_projects,
  (stats_used['service_desk_issues']::numeric)                       AS service_desk_issues,
  (stats_used['snippets']::numeric)                                  AS snippets,
  (stats_used['todos']::numeric)                                     AS todos,
  (stats_used['uploads']::numeric)                                   AS uploads,
  (stats_used['web_hooks']::numeric)                                 AS web_hooks
FROM usage_data
{% if is_incremental() %}
    WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
