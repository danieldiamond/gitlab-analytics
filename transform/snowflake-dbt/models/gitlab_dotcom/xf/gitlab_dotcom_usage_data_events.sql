{%- set event_ctes = [
  {
    "event_name": "boards",
    "table_name": "gitlab_dotcom_boards",
    "key_to_parent_object": "project_id",
    "primary_key": "board_id"
  },
  {
    "event_name": "ci_builds",
    "table_name": "gitlab_dotcom_ci_builds",
    "key_to_parent_object": "ci_build_project_id",
    "primary_key": "ci_build_id"
  },
  {
    "event_name": "ci_pipeline_schedules",
    "table_name": "gitlab_dotcom_ci_pipeline_schedules",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_pipeline_schedule_id"
  },
  {
    "event_name": "ci_pipelines",
    "table_name": "gitlab_dotcom_ci_pipelines",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_pipeline_id"
  },
  {
    "event_name": "ci_stages",
    "table_name": "gitlab_dotcom_ci_stages",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_stage_id"
  },
  {
    "event_name": "ci_triggers",
    "table_name": "gitlab_dotcom_ci_triggers",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_trigger_id"
  },
  {
    "event_name": "deployments",
    "table_name": "gitlab_dotcom_deployments",
    "key_to_parent_object": "project_id",
    "primary_key": "deployment_id"
  },
  {
    "event_name": "environments",
    "table_name": "gitlab_dotcom_environments",
    "key_to_parent_object": "project_id",
    "primary_key": "environment_id"
  },
  {
    "event_name": "issues",
    "table_name": "gitlab_dotcom_issues",
    "key_to_parent_object": "project_id",
    "primary_key": "issue_id"
  },
  {
    "event_name": "labels",
    "table_name": "gitlab_dotcom_labels",
    "key_to_parent_object": "project_id",
    "primary_key": "label_id"
  },
  {
    "event_name": "lfs_objects",
    "table_name": "gitlab_dotcom_lfs_objects_projects",
    "key_to_parent_object": "project_id",
    "primary_key": "lfs_object_id"
  },
  {
    "event_name": "merge_requests",
    "table_name": "gitlab_dotcom_merge_requests",
    "key_to_parent_object": "project_id",
    "primary_key": "merge_request_id"
  },
  {
    "event_name": "milestones",
    "table_name": "gitlab_dotcom_milestones",
    "key_to_parent_object": "project_id",
    "primary_key": "milestone_id"
  },
  {
    "event_name": "notes",
    "table_name": "gitlab_dotcom_notes",
    "key_to_parent_object": "project_id",
    "primary_key": "note_id"
  },
  {
    "event_name": "project_auto_devops",
    "table_name": "gitlab_dotcom_project_auto_devops",
    "key_to_parent_object": "project_id",
    "primary_key": "project_auto_devops_id"
  },
  {
    "event_name": "releases",
    "table_name": "gitlab_dotcom_releases",
    "key_to_parent_object": "project_id",
    "primary_key": "release_id"
  },
  {
    "event_name": "snippets",
    "table_name": "gitlab_dotcom_snippets",
    "key_to_parent_object": "project_id",
    "primary_key": "snippet_id"
  },
  {
    "event_name": "todos",
    "table_name": "gitlab_dotcom_todos",
    "key_to_parent_object": "project_id",
    "primary_key": "todo_id"
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
  FROM {{ ref('gitlab_dotcom_projects') }}
  
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
  projects.created_at                            AS project_created_at,
  {{ event_cte.event_name }}.created_at          AS event_created_at,
  '{{ event_cte.event_name }}'                   AS event_name,
  CASE
    WHEN gitlab_subscriptions.is_trial
      THEN 'trial'
    ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
  END                                            AS plan_id_at_event_date,
  DATEDIFF('hour', 
           ultimate_namespace.namespace_created_at, 
           event_created_at)/24::INTEGER         AS days_since_namespace_creation,
  DATEDIFF('hour', 
           ultimate_namespace.namespace_created_at, 
           event_created_at)/(24 * 7)::INTEGER   AS weeks_since_namespace_creation,
  DATEDIFF('hour', 
           project_created_at, 
           event_created_at)/24::INTEGER         AS days_since_project_creation,
  DATEDIFF('hour', 
           project_created_at, 
           event_created_at)/(24 * 7)::INTEGER   AS weeks_since_project_creation
FROM {{ event_cte.event_name }}
  INNER JOIN projects ON {{ event_cte.event_name }}.{{event_cte.key_to_parent_object}} = projects.project_id
  INNER JOIN namespaces ON projects.namespace_id = namespaces.namespace_id
  INNER JOIN namespaces AS ultimate_namespace 
    ON namespaces.namespace_ultimate_parent_id = ultimate_namespace.namespace_id
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
