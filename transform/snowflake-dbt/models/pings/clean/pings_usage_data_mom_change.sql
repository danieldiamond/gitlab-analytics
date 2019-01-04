{{
  config(
    materialized='incremental',
    sql_where='TRUE',
    unique_key='unique_key'
  )
}}

WITH pings_mom_change AS (
  SELECT
    md5(uuid || created_at)             AS unique_key,
    uuid                                AS uuid,
    created_at                          AS created_at,
    edition                             AS edition,
    {{ mom_change_count('assignee_lists') }} ,
    {{ mom_is_used('auto_devops_disabled') }} ,
    {{ mom_is_used('auto_devops_enabled') }} ,
    {{ mom_change_count('boards') }} ,
    {{ mom_change_count('ci_builds') }} ,
    {{ mom_change_count('ci_external_pipelines') }} ,
    {{ mom_change_count('ci_internal_pipelines') }} ,
    {{ mom_change_count('ci_pipeline_config_auto_devops') }} ,
    {{ mom_change_count('ci_pipeline_config_repository') }} ,
    {{ mom_change_count('ci_pipeline_schedules') }} ,
    {{ mom_is_used('ci_runners') }} ,
    {{ mom_change_count('ci_triggers') }} ,
    {{ mom_change_count('clusters') }} ,
    {{ mom_change_count('clusters_applications_helm') }} ,
    {{ mom_change_count('clusters_applications_ingress') }} ,
    {{ mom_change_count('clusters_applications_prometheus') }} ,
    {{ mom_change_count('clusters_applications_runner') }} ,
    {{ mom_is_used('clusters_disabled') }} ,
    {{ mom_is_used('clusters_enabled') }} ,
    {{ mom_change_count('clusters_platforms_gke') }} ,
    {{ mom_change_count('clusters_platforms_user') }} ,
    {{ mom_change_count('container_scanning_jobs') }} ,
    {{ mom_change_count('dependency_scanning_jobs') }} ,
    {{ mom_change_count('deploy_keys') }} ,
    {{ mom_change_count('deployments') }} ,
    {{ mom_change_count('environments') }} ,
    {{ mom_change_count('epics') }} ,
    {{ mom_change_count('gcp_clusters') }} ,
    {{ mom_change_count('geo_nodes') }} ,
    {{ mom_change_count('groups') }} ,
    {{ mom_change_count('in_review_folder') }} ,
    {{ mom_change_count('issues') }} ,
    {{ mom_change_count('keys') }} ,
    {{ mom_change_count('label_lists') }} ,
    {{ mom_change_count('labels') }} ,
    {{ mom_change_count('ldap_group_links') }} ,
    {{ mom_change_count('ldap_keys') }} ,
    {{ mom_change_count('ldap_users') }} ,
    {{ mom_change_count('lfs_objects') }} ,
    {{ mom_change_count('license_management_jobs') }} ,
    {{ mom_change_count('merge_requests') }} ,
    {{ mom_change_count('milestone_lists') }} ,
    {{ mom_change_count('milestones') }} ,
    {{ mom_change_count('notes') }} ,
    {{ mom_change_count('pages_domains') }} ,
    {{ mom_change_count('projects') }} ,
    {{ mom_change_count('projects_imported_from_github') }} ,
    {{ mom_change_count('projects_jira_active') }} ,
    {{ mom_change_count('projects_mirrored_with_pipelines_enabled') }} ,
    {{ mom_change_count('projects_prometheus_active') }} ,
    {{ mom_change_count('projects_reporting_ci_cd_back_to_github') }} ,
    {{ mom_is_used('projects_slack_notifications_active') }} ,
    {{ mom_is_used('projects_slack_slash_active') }} ,
    {{ mom_change_count('protected_branches') }} ,
    {{ mom_change_count('releases') }} ,
    {{ mom_change_count('remote_mirrors') }} ,
    {{ mom_change_count('sast_jobs') }} ,
    {{ mom_change_count('service_desk_enabled_projects') }} ,
    {{ mom_change_count('service_desk_issues') }} ,
    {{ mom_change_count('snippets') }} ,
    {{ mom_change_count('todos') }} ,
    {{ mom_change_count('uploads') }} ,
    {{ mom_change_count('web_hooks') }}
  FROM {{ ref("pings_usage_data_month") }}
  {% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
      WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE) - interval '1 month'
  {% endif %}
)

SELECT *
FROM pings_mom_change
