-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}

{% set fields_to_mask = ['epic_title', 'epic_description'] %}


WITH epics AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_epics')}}

), label_links AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_label_links')}}
    WHERE is_currently_valid = True
      AND target_type = 'Epic' --TODO test

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels AS (

    SELECT
      epics.epic_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC) AS labels
    FROM epics
    LEFT JOIN label_links
      ON epics.issue_id = label_links.target_id
    LEFT JOIN all_labels
      ON label_links.label_id = all_labels.label_id
    GROUP BY epics.epic_id

), projects AS (

    SELECT
      project_id,
      namespace_id,
      visibility_level
    FROM {{ref('gitlab_dotcom_projects')}}

), namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

, gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base')}}
),

joined AS (

  SELECT
    {{ dbt_utils.star(from=ref('gitlab_dotcom_epics'), except=fields_to_mask|upper) }}
    
    {% for field in fields_to_mask %}
    CASE
      WHEN is_confidential = TRUE
        AND namespace_lineage.namespace_is_internal = TRUE
        THEN 'confidential - masked'
      WHEN visibility_level != 'public'
        AND namespace_lineage.namespace_is_internal = TRUE
        THEN 'private/internal - masked'
      ELSE {{field}}
    END                                          AS {{field}},
    {% endfor %}

   
    IFF(issues.project_id IN ({{is_project_included_in_engineering_metrics()}}),
      TRUE, FALSE)                               AS is_included_in_engineering_metrics,
    IFF(issues.project_id IN ({{is_project_part_of_product()}}),
      TRUE, FALSE)                               AS is_part_of_product,
    state,
    weight,
    due_date,
    lock_version,
    time_estimate,
    has_discussion_locked,
    closed_by_id,
    relative_position,
    service_desk_reply_to,
    duplicated_to_id,
    promoted_to_epic_id,

    agg_labels.labels,
    ARRAY_TO_STRING(agg_labels.labels,'|')       AS masked_label_title,

    namespace_lineage.namespace_is_internal      AS is_internal_epic,
    namespace_lineage.ultimate_parent_id,
    namespace_lineage.ultimate_parent_plan_id,
    namespace_lineage.ultimate_parent_plan_title,
    namespace_lineage.ultimate_parent_plan_is_paid,

    CASE
      WHEN gitlab_subscriptions.is_trial
        THEN 'trial'
      ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
    END AS plan_id_at_issue_creation

  FROM epics
  LEFT JOIN agg_labels
    ON issues.issue_id = agg_labels.issue_id
  LEFT JOIN projects
    ON issues.project_id = projects.project_id
  LEFT JOIN namespace_lineage
    ON projects.namespace_id = namespace_lineage.namespace_id
  LEFT JOIN gitlab_subscriptions
    ON namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
    AND issues.created_at BETWEEN gitlab_subscriptions.valid_from AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
)

SELECT *
FROM joined