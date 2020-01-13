-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}

{% set fields_to_mask = ['issue_title', 'issue_description'] %}


WITH issues AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_issues')}}

), label_states AS (

    SELECT
      label_id,
      issue_id
    FROM {{ref('gitlab_dotcom_label_states_xf')}}
    WHERE issue_id IS NOT NULL
      AND latest_state = 'added'

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels AS (

    SELECT
      issues.issue_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC) AS labels
    FROM issues
    LEFT JOIN label_states
      ON issues.issue_id = label_states.issue_id
    LEFT JOIN all_labels
      ON label_states.label_id = all_labels.label_id
    GROUP BY issues.issue_id

), projects AS (

    SELECT
      project_id,
      namespace_id,
      visibility_level
    FROM {{ref('gitlab_dotcom_projects')}}

), namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}
),

joined AS (

  SELECT
    issues.issue_id,
    issues.issue_iid,
    issues.author_id,
    issues.project_id,
    milestone_id,
    updated_by_id,
    last_edited_by_id,
    moved_to_id,
    issues.created_at                            AS issue_created_at,
    issues.updated_at                            AS issue_updated_at,
    issue_last_edited_at,
    issue_closed_at,
    projects.namespace_id,
    visibility_level,
    is_confidential                              AS issue_is_confidential,
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

    CASE
    WHEN projects.namespace_id = 9970
      AND ARRAY_CONTAINS('community contribution'::variant, agg_labels.labels)
      THEN TRUE
    ELSE FALSE
    END                                          AS is_community_contributor_related,

    CASE
      WHEN ARRAY_CONTAINS('s1'::variant, agg_labels.labels)
        THEN 'severity 1'
      WHEN ARRAY_CONTAINS('s2'::variant, agg_labels.labels)
        THEN 'severity 2'
      WHEN ARRAY_CONTAINS('s3'::variant, agg_labels.labels)
        THEN 'severity 3'
      WHEN ARRAY_CONTAINS('s4'::variant, agg_labels.labels)
        THEN 'severity 4'
      ELSE 'undefined'
    END                                          AS severity_tag,

    CASE
      WHEN ARRAY_CONTAINS('p1'::variant, agg_labels.labels) THEN 'priority 1'
      WHEN ARRAY_CONTAINS('p2'::variant, agg_labels.labels) THEN 'priority 2'
      WHEN ARRAY_CONTAINS('p3'::variant, agg_labels.labels) THEN 'priority 3'
      WHEN ARRAY_CONTAINS('p4'::variant, agg_labels.labels) THEN 'priority 4'
      ELSE 'undefined'
    END                                          AS priority_tag,

    CASE
      WHEN projects.namespace_id = 9970
        AND ARRAY_CONTAINS('security'::variant, agg_labels.labels)
        THEN TRUE
      ELSE FALSE
    END                                          AS is_security_issue,

    IFF(issues.project_id IN ({{is_project_included_in_engineering_metrics()}}),
      TRUE, FALSE)                               AS is_included_in_engineering_metrics,
    IFF(issues.project_id IN ({{is_project_part_of_product()}}),
      TRUE, FALSE)                               AS is_part_of_product,
    issue_state                                  AS state,
    weight,
    due_date,
    lock_version,
    time_estimate,
    has_discussion_locked,
    agg_labels.labels,
    ARRAY_TO_STRING(agg_labels.labels,'|')       AS masked_label_title,

    namespace_lineage.namespace_is_internal      AS is_internal_issue,
    namespace_lineage.namespace_plan_id,
    namespace_lineage.namespace_plan_title,
    namespace_lineage.namespace_plan_is_paid,
    namespace_lineage.ultimate_parent_id,
    namespace_lineage.ultimate_parent_plan_id,
    namespace_lineage.ultimate_parent_plan_title,
    namespace_lineage.ultimate_parent_plan_is_paid,

    CASE
      WHEN issue_created_at BETWEEN DATEADD('days', -30, gitlab_subscription_trial_ends_on) AND gitlab_subscription_trial_ends_on
        THEN 'trial'
      ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
    END AS namespace_plan_id_at_issue_creation

  FROM issues
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
