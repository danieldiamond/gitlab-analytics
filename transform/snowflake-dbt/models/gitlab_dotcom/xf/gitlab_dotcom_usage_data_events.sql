{%- set event_ctes = [
  {
    "event_name": "ci_builds",
    "table_name": "gitlab_dotcom_ci_builds",
    "key_to_parent_object": "ci_build_project_id",
    "primary_key": "ci_build_id"
  },
  {
    "event_name": "notes",
    "table_name": "gitlab_dotcom_notes",
    "key_to_parent_object": "note_project_id",
    "primary_key": "note_id"
  },
  {
    "event_name": "ci_pipelines",
    "table_name": "gitlab_dotcom_ci_pipelines",
    "key_to_parent_object": "project_id",
    "primary_key": "ci_pipeline_id"
  },
  {
    "event_name": "merge_requests",
    "table_name": "gitlab_dotcom_merge_requests",
    "key_to_parent_object": "project_id",
    "primary_key": "merge_request_id"
  },
  {
    "event_name": "todos",
    "table_name": "gitlab_dotcom_todos",
    "key_to_parent_object": "project_id",
    "primary_key": "todo_id"
  },
  {
    "event_name": "lfs_objects",
    "table_name": "gitlab_dotcom_lfs_objects_projects",
    "key_to_parent_object": "project_id",
    "primary_key": "lfs_object_id"
  },
  {
    "event_name": "deployments",
    "table_name": "gitlab_dotcom_deployments",
    "key_to_parent_object": "project_id",
    "primary_key": "deployment_id"
  }
]
-%}

WITH namespaces AS (
  
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

{% for event_cte in event_ctes %}

, {{ event_cte.event_name }} AS (
  
  SELECT *
  FROM {{ ref(event_cte.table_name) }}
  
)

{% endfor -%}

{% for event_cte in event_ctes %}

SELECT
  ultimate_namespace.namespace_id, 
  ultimate_namespace.namespace_created_at,
  projects.created_at                   AS project_created_at,
  {{ event_cte.event_name }}.created_at AS event_created_at
FROM {{ event_cte.event_name }}
LEFT JOIN projects ON {{ event_cte.event_name }}.{{event_cte.key_to_parent_object}} = projects.project_id
LEFT JOIN namespaces ON projects.namespace_id = namespaces.namespace_id
LEFT JOIN namespaces AS ultimate_namespace 
  ON namespaces.namespace_ultimate_parent_id = ultimate_namespace.namespace_id

{% if not loop.last %} 
UNION
{% endif %}

{% endfor -%}
