{{
  config({
    "materialized": "incremental"
  })
}}

/*
  Each dict must have ALL of the following:
    * event_name
    * primary_key
    * is_representative_of_stage
    * primary_key
  Must have ONE of the following:
    * source_cte_name OR source_table_name
    * key_to_parent_project OR key_to_group_project (NOT both, see how clusters_applications_helm is included twice for group and project.
*/

{%- set event_ctes = [
  {
    "event_name": "boards",
    "source_table_name": "gitlab_dotcom_boards",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "board_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_builds",
    "source_table_name": "gitlab_dotcom_ci_builds",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_pipeline_schedules",
    "source_table_name": "gitlab_dotcom_ci_pipeline_schedules",
    "user_column_name": "owner_id",
    "key_to_parent_project": "project_id",
    "primary_key": "ci_pipeline_schedule_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_pipelines",
    "source_table_name": "gitlab_dotcom_ci_pipelines",
    "user_column_name": "user_id",
    "key_to_parent_project": "project_id",
    "primary_key": "ci_pipeline_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "ci_stages",
    "source_table_name": "gitlab_dotcom_ci_stages",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "ci_stage_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "ci_triggers",
    "source_table_name": "gitlab_dotcom_ci_triggers",
    "user_column_name": "owner_id",
    "key_to_parent_project": "project_id",
    "primary_key": "ci_trigger_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "clusters_applications_helm",
    "source_table_name": "gitlab_dotcom_clusters_applications_helm_xf",
    "user_column_name": "user_id",
    "key_to_parent_project": "cluster_project_id",
    "primary_key": "clusters_applications_helm_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "clusters_applications_helm",
    "source_table_name": "gitlab_dotcom_clusters_applications_helm_xf",
    "user_column_name": "user_id",
    "key_to_parent_group": "cluster_group_id",
    "primary_key": "clusters_applications_helm_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "deployments",
    "source_table_name": "gitlab_dotcom_deployments",
    "user_column_name": "user_id",
    "key_to_parent_project": "project_id",
    "primary_key": "deployment_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "environments",
    "source_table_name": "gitlab_dotcom_environments",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "environment_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "epics",
    "source_table_name": "gitlab_dotcom_epics",
    "user_column_name": "author_id",
    "key_to_parent_group": "group_id",
    "primary_key": "epic_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "groups",
    "source_cte_name": "group_members",
    "user_column_name": "user_id",
    "key_to_parent_project": "source_id",
    "primary_key": "member_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "issues",
    "source_table_name": "gitlab_dotcom_issues",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "issue_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "labels",
    "source_table_name": "gitlab_dotcom_labels",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "label_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "lfs_objects",
    "source_table_name": "gitlab_dotcom_lfs_objects_projects",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "lfs_object_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "merge_requests",
    "source_table_name": "gitlab_dotcom_merge_requests",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "merge_request_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "milestones",
    "source_table_name": "gitlab_dotcom_milestones",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "milestone_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "notes",
    "source_table_name": "gitlab_dotcom_notes",
    "user_column_name": "note_author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "note_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "project_auto_devops",
    "source_table_name": "gitlab_dotcom_project_auto_devops",
    "user_column_name": "NULL",
    "key_to_parent_project": "project_id",
    "primary_key": "project_auto_devops_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "projects_container_registry_enabled",
    "source_cte_name": "projects_container_registry_enabled",
    "user_column_name": "creator_id",
    "key_to_parent_project": "project_id",
    "primary_key": "project_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "projects_prometheus_active",
    "source_cte_name": "projects_prometheus_active",
    "user_column_name": "creator_id",
    "key_to_parent_project": "project_id",
    "primary_key": "project_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "releases",
    "source_table_name": "gitlab_dotcom_releases",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "release_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "secure_stage_ci_jobs",
    "source_table_name": "gitlab_dotcom_secure_stage_ci_jobs",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "snippets",
    "source_table_name": "gitlab_dotcom_snippets",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "snippet_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "todos",
    "source_table_name": "gitlab_dotcom_todos",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "todo_id",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "users",
    "source_table_name": "gitlab_dotcom_users",
    "user_column_name": "user_id",
    "primary_key": "user_id",
    "is_representative_of_stage": "True"
  },
]
-%}


WITH gitlab_subscriptions AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base') }}
)

, plans AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans') }}

)

, namespaces AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

)

, projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_xf') }}

)

, users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users') }}

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

), projects_container_registry_enabled AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_xf') }}
    WHERE container_registry_enabled = True

), group_members AS (

    SELECT
      *,
      invite_created_at AS created_at
    FROM {{ ref('gitlab_dotcom_members') }}
    WHERE member_source_type = 'Namespace'

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
    WHERE created_at IS NOT NULL
      {% if is_incremental() %}
        AND created_at >= (SELECT MAX(event_created_at) FROM {{this}} WHERE event_name = '{{ event_cte.event_name }}')
      {% endif %}

)

{% endfor -%}

, data AS (

{% for event_cte in event_ctes %}

    SELECT
      ultimate_namespace.namespace_id,
      ultimate_namespace.namespace_created_at,
      {% if 'NULL' in event_cte.user_column_name %}
        NULL
      {% else %}
        {{ event_cte.event_name }}.{{ event_cte.user_column_name }}
      {% endif %}                                                 AS user_id,
      {% if event_cte.key_to_parent_project is defined %}
        'project'                                                 AS parent_type,
        projects.project_id                                       AS parent_id,
        projects.project_created_at                               AS parent_created_at,
      {% elif event_cte.key_to_parent_group is defined %}
        'group'                                                   AS parent_type,
        namespaces.namespace_id                                   AS parent_id,
        namespaces.namespace_created_at                           AS parent_created_at,
      {% else %}
        NULL                                                      AS parent_type,
        NULL                                                      AS parent_id,
        NULL                                                      AS parent_created_at,
      {% endif %}
      {{ event_cte.event_name }}.created_at                       AS event_created_at,
      {{ event_cte.is_representative_of_stage }}::BOOLEAN         AS is_representative_of_stage,
      '{{ event_cte.event_name }}'                                AS event_name,
      CASE
        WHEN '{{ event_cte.event_name }}' = 'project_auto_devops'
          THEN 'configure'
        WHEN '{{ event_cte.event_name }}' = 'ci_stages'
          THEN 'configure'
        WHEN '{{ event_cte.event_name }}' = 'users'
          THEN 'manage'
        WHEN '{{ event_cte.event_name }}' = 'projects_container_registry_enabled'
          THEN 'package'
        ELSE version_usage_stats_to_stage_mappings.stage
      END                                                         AS stage_name,
      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
      END                                                         AS plan_id_at_event_date,
      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(plans.plan_name, 'free')
      END                                                         AS plan_name_at_event_date
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

      -- Join on either the project's or the group's ultimate namespace.
      INNER JOIN namespaces AS ultimate_namespace
        {% if event_cte.key_to_parent_project is defined %}
        ON ultimate_namespace.namespace_id = projects.ultimate_parent_id
        {% elif event_cte.key_to_parent_group is defined %}
        ON ultimate_namespace.namespace_id = namespaces.namespace_ultimate_parent_id
        {% else %}
        ON FALSE -- Don't join any rows.
        {% endif %}

      LEFT JOIN gitlab_subscriptions
        ON ultimate_namespace.namespace_id = gitlab_subscriptions.namespace_id
        AND {{ event_cte.event_name }}.created_at BETWEEN gitlab_subscriptions.valid_from
        AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
      LEFT JOIN plans
        ON gitlab_subscriptions.plan_id = plans.plan_id
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
      users.created_at                                    AS user_created_at,
      FLOOR(
      DATEDIFF('hour',
              namespace_created_at,
              event_created_at)/24)                       AS days_since_namespace_creation,
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
                event_created_at)/(24 * 7))               AS weeks_since_parent_creation,
      FLOOR(
        DATEDIFF('hour',
                user_created_at,
                event_created_at)/24)                     AS days_since_user_creation,
      FLOOR(
        DATEDIFF('hour',
                user_created_at,
                event_created_at)/(24 * 7))               AS weeks_since_user_creation
    FROM data
      LEFT JOIN users
        ON data.user_id = users.user_id
)

SELECT *
FROM final
