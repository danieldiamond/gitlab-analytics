{% set fields_to_mask = ['epic_title', 'epic_description'] %}

/* Code is sourced from gitlab_dotcom_issues_xf */
WITH epics AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_epics')}}

), label_links AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_label_links')}}
    WHERE is_currently_valid = True
      AND target_type = 'Epic'

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels AS (

    SELECT
      epics.epic_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC) AS labels
    FROM epics
    LEFT JOIN label_links
      ON epics.epic_id = label_links.target_id
    LEFT JOIN all_labels
      ON label_links.label_id = all_labels.label_id
    GROUP BY epics.epic_id

), namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces')}}

), namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

) , gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base')}}
),

joined AS (

  SELECT
    {{ dbt_utils.star(from=ref('gitlab_dotcom_epics'), except=fields_to_mask|upper, relation_alias='epics')}},
    
    {% for field in fields_to_mask %}
    CASE
      WHEN {{field}} = NULL
        THEN NULL
      WHEN namespaces.visibility_level = 'public'
        THEN {{field}}
      WHEN namespace_lineage.namespace_is_internal = True
        THEN {{field}}
      ELSE 'private/internal - masked'
    END                                          AS {{field}},
    {% endfor %}

    agg_labels.labels,

    namespaces.visibility_level                  AS namespace_visibility_level,
    namespace_lineage.namespace_is_internal      AS is_internal_epic,
    namespace_lineage.ultimate_parent_id,
    namespace_lineage.ultimate_parent_plan_id,
    namespace_lineage.ultimate_parent_plan_title,
    namespace_lineage.ultimate_parent_plan_is_paid,

    CASE
      WHEN gitlab_subscriptions.is_trial
        THEN 'trial'
      ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
    END AS plan_id_at_epic_creation

  FROM epics
  LEFT JOIN agg_labels
    ON epics.epic_id = agg_labels.epic_id
  LEFT JOIN namespaces
    ON epics.group_id = namespaces.namespace_id
  LEFT JOIN namespace_lineage
    ON epics.group_id = namespace_lineage.namespace_id
  LEFT JOIN gitlab_subscriptions
    ON namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
    AND epics.created_at BETWEEN gitlab_subscriptions.valid_from AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
)

SELECT *
FROM joined
