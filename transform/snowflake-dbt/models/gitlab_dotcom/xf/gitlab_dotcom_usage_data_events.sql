{%- set event_ctes = [
  {
    "event_name": "boards",
    "table_name": "gitlab_dotcom_boards",
    "key_to_parent_object": "project_id",
    "primary_key": "board_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "clusters_applications_helm",
    "table_name": "gitlab_dotcom_clusters_applications_helm_xf",
    "key_to_parent_project": "cluster_project_id",
    "key_to_parent_group": "cluster_group_id",
    "primary_key": "clusters_applications_helm_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "ci_builds",
    "table_name": "gitlab_dotcom_ci_builds",
    "key_to_parent_object": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_pipeline_schedules",
    "table_name": "gitlab_dotcom_ci_pipeline_schedules",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_pipeline_schedule_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_pipelines",
    "table_name": "gitlab_dotcom_ci_pipelines",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_pipeline_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_stages",
    "table_name": "gitlab_dotcom_ci_stages",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_stage_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_triggers",
    "table_name": "gitlab_dotcom_ci_triggers",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_trigger_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "deployments",
    "table_name": "gitlab_dotcom_deployments",
    "key_to_parent_object": "project_id",
    "primary_key": "deployment_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "environments",
    "table_name": "gitlab_dotcom_environments",
    "key_to_parent_object": "project_id",
    "primary_key": "environment_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "issues",
    "table_name": "gitlab_dotcom_issues",
    "key_to_parent_object": "project_id",
    "primary_key": "issue_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "labels",
    "table_name": "gitlab_dotcom_labels",
    "key_to_parent_object": "project_id",
    "primary_key": "label_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "lfs_objects",
    "table_name": "gitlab_dotcom_lfs_objects_projects",
    "key_to_parent_object": "project_id",
    "primary_key": "lfs_object_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "merge_requests",
    "table_name": "gitlab_dotcom_merge_requests",
    "key_to_parent_object": "project_id",
    "primary_key": "merge_request_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "milestones",
    "table_name": "gitlab_dotcom_milestones",
    "key_to_parent_object": "project_id",
    "primary_key": "milestone_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "notes",
    "table_name": "gitlab_dotcom_notes",
    "key_to_parent_object": "project_id",
    "primary_key": "note_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "project_auto_devops",
    "table_name": "gitlab_dotcom_project_auto_devops",
    "key_to_parent_object": "project_id",
    "primary_key": "project_auto_devops_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "releases",
    "table_name": "gitlab_dotcom_releases",
    "key_to_parent_object": "project_id",
    "primary_key": "release_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "snippets",
    "table_name": "gitlab_dotcom_snippets",
    "key_to_parent_object": "project_id",
    "primary_key": "snippet_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "todos",
    "table_name": "gitlab_dotcom_todos",
    "key_to_parent_object": "project_id",
    "primary_key": "todo_id",
    "is_representative_of_stage": "False"
  }
]
-%}

WITH gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base')}}
)

, namespaces AS (
  
  SELECT * 
  FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

)

, projects AS (
  
  SELECT *
  FROM {{ ref('gitlab_dotcom_projects_xf') }}
  
)

, label_events AS (
  
  SELECT *
  FROM {{ ref('gitlab_dotcom_labels') }}
  
)

, version_usage_stats_to_stage_mappings AS (
  
  SELECT *
  FROM {{ ref('version_usage_stats_to_stage_mappings') }}
)

{% for event_cte in event_ctes %}

, {{ event_cte.event_name }} AS (
  
  SELECT *
  FROM {{ ref(event_cte.table_name) }}
  {% if is_incremental() %}

  WHERE created_at >= (SELECT MAX(event_created_at) FROM {{this}} WHERE event_name = '{{ event_cte.event_name }}')

  {% endif %}
  
)

{% endfor -%}

{% for event_cte in event_ctes %}

SELECT
  ultimate_namespace.namespace_id, 
  ultimate_namespace.namespace_created_at,
  projects.project_id,
  projects.project_created_at,
  {{ event_cte.event_name }}.created_at                AS event_created_at,
  {{ event_cte.is_representative_of_stage }}::BOOLEAN  AS is_representative_of_stage,
  
  '{{ event_cte.event_name }}'                         AS event_name,
  CASE
    WHEN '{{ event_cte.event_name }}' = 'project_auto_devops'
      THEN 'configure'
    WHEN '{{ event_cte.event_name }}' = 'ci_stages'
      THEN 'configure'
    ELSE version_usage_stats_to_stage_mappings.stage    
  END                                                  AS stage_name,
  CASE
    WHEN gitlab_subscriptions.is_trial
      THEN 'trial'
    ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
  END                                                  AS plan_id_at_event_date,
  FLOOR(
    DATEDIFF('hour', 
             ultimate_namespace.namespace_created_at, 
             event_created_at)/24)                     AS days_since_namespace_creation,
  FLOOR(
    DATEDIFF('hour', 
             ultimate_namespace.namespace_created_at, 
             event_created_at)/(24 * 7))               AS weeks_since_namespace_creation,
  FLOOR(
    DATEDIFF('hour', 
             project_created_at, 
             event_created_at)/24)                     AS days_since_project_creation,
  FLOOR(
    DATEDIFF('hour', 
             project_created_at, 
             event_created_at)/(24 * 7))               AS weeks_since_project_creation
FROM {{ event_cte.event_name }}
  /* Join with project project. */
  LEFT JOIN projects
    ON {{ event_cte.event_name }}.{{event_cte.key_to_parent_project}} = projects.project_id
  /* Join with parent group. */
  LEFT JOIN namespaces
    ON {{ event_cte.event_name }}.{{event_cte.key_to_parent_group}} = namespaces.namespace_id
  /* Join on either the project or the group's ultimate namespace. */
  INNER JOIN namespaces AS ultimate_namespace 
    ON COALESCE(projects.ultimate_parent_id, namespaces.namespace_ultimate_parent_id) = ultimate_namespace.namespace_id
  LEFT JOIN gitlab_subscriptions
    ON ultimate_namespace.namespace_id = gitlab_subscriptions.namespace_id
    AND {{ event_cte.event_name }}.created_at BETWEEN gitlab_subscriptions.valid_from 
    AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
  LEFT JOIN version_usage_stats_to_stage_mappings
    ON '{{ event_cte.event_name }}' = version_usage_stats_to_stage_mappings.stats_used_key_name

{% if not loop.last %} 
UNION
{% endif %}

{% endfor -%}
