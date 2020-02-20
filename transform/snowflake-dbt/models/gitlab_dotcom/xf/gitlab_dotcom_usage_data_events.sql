/* 
  Must have ALL of the following:
    * event_name
    * primary_key
    * is_representative_of_stage
    * primary_key
  Must have ONE of the following:
    * source_cte_name OR source_table_name
    * key_to_parent_project or key_to_group_project (or both)
*/

{%- set event_ctes = [
  {
    "event_name": "clusters_applications_helm",
    "source_table_name": "gitlab_dotcom_clusters_applications_helm_xf",
    "key_to_parent_group": "cluster_group_id",
    "primary_key": "clusters_applications_helm_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "clusters_applications_helm",
    "source_table_name": "gitlab_dotcom_clusters_applications_helm_xf",
    "key_to_parent_project": "cluster_project_id",
    "primary_key": "clusters_applications_helm_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "projects_prometheus_active",
    "source_cte_name": "projects_prometheus_active",
    "key_to_parent_project": "project_id",
    "primary_key": "project_id",
    "is_representative_of_stage": "True"
  },
]
-%}

/* Temporary excluded
  {
    "event_name": "ci_builds",
    "source_table_name": "gitlab_dotcom_ci_builds",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "is_representative_of_stage": "False"
  },
*/

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

/* Source CTEs Start Here */ 
, projects_prometheus_active AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_projects_xf') }}
  WHERE ARRAY_CONTAINS('PrometheusService'::VARIANT, active_service_types)

)
/* End of Source CTEs */

{% for event_cte in event_ctes %}

, {{ event_cte.event_name }} AS (
  
  SELECT *
  /* Check for source_table_name, else use source_cte_name. */
  {% if event_cte.source_table_name is defined %}
    FROM {{ ref(event_cte.source_table_name) }}
  {% else %}
    FROM {{ event_cte.source_cte_name }}
  {% endif %}

  {% if is_incremental() %}

  WHERE created_at >= (SELECT MAX(event_created_at) FROM {{this}} WHERE event_name = '{{ event_cte.event_name }}')

  {% endif %}
  
)

{% endfor -%}

, data AS (

{% for event_cte in event_ctes %}

SELECT
  ultimate_namespace.namespace_id, 
  ultimate_namespace.namespace_created_at,
  {% if event_cte.key_to_parent_project is defined %}
    'project'                     AS parent_type,
    projects.project_id           AS parent_id,
    projects.project_created_at   AS parent_created_at,
  {% elif event_cte.key_to_parent_group is defined %}
    'group'                       AS parent_type,
    namespaces.namespace_id       AS parent_id,
    namespaces.project_created_at AS parent_created_at,
  {% endif %}
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
  END                                                  AS plan_id_at_event_date
FROM {{ event_cte.event_name }}
  /* Join with parent project. */
  {% if event_cte.key_to_parent_project is defined %}
  LEFT JOIN projects
    ON {{ event_cte.event_name }}.{{ event_cte.key_to_parent_project }} = projects.project_id
  /* Join with parent group. */
  {% elif event_cte.key_to_parent_group is defined %}
  LEFT JOIN namespaces
    ON {{ event_cte.event_name }}.{{ event_cte.key_to_parent_group }} = namespaces.namespace_id
  {% endif %}
  
  -- Join on either the project or the group's ultimate namespace.
  INNER JOIN namespaces AS ultimate_namespace 
    ON ultimate_namespace.namespace_id = 
    {% if event_cte.key_to_parent_project is defined %}
    projects.ultimate_parent_id
    {% elif event_cte.key_to_parent_group is defined %}
    namespaces.namespace_ultimate_parent_id
    {% endif %}

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

)

, final AS (
  SELECT
    data.*,
    FLOOR(
    DATEDIFF('hour', 
             namespace_created_at, 
             event_created_at)/24)                      AS days_since_namespace_creation,
    FLOOR(
      DATEDIFF('hour', 
              namespace_created_at, 
              event_created_at)/(24 * 7))               AS weeks_since_namespace_creation,
    FLOOR(
      DATEDIFF('hour', 
              parent_created_at, 
              event_created_at)/24)                     AS days_since_parent_creation,
    FLOOR(
      DATEDIFF('hour', 
              parent_created_at, 
              event_created_at)/(24 * 7))               AS weeks_since_parent_creation
  FROM data
)

SELECT *
FROM data