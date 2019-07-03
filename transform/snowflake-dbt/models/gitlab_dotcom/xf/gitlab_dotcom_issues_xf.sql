-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}

{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

{% set fields_to_mask = ['title', 'description'] %}


with issues as (

    SELECT * FROM {{ref('gitlab_dotcom_issues')}}

), all_label_links as (

    SELECT * FROM {{ref('gitlab_dotcom_label_links')}}

), label_links as (

    SELECT *
    FROM all_label_links
    WHERE target_type = 'Issue'

), all_labels as (

    SELECT * FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels as (

    SELECT
      issue_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY issue_id ASC) AS agg_label
    FROM issues
    LEFT JOIN label_links
    ON issues.issue_id = label_links.target_id
    LEFT JOIN all_labels
    ON label_links.label_id = all_labels.label_id
    GROUP BY issue_id

), projects as (

    SELECT project_id,
           namespace_id,
           visibility_level
    FROM {{ref('gitlab_dotcom_projects')}}

), joined as (

    SELECT issues.issue_id,
           issues.issue_iid,
           author_id,
           issues.project_id,
           milestone_id,
           updated_by_id,
           last_edited_by_id,
           moved_to_id,
           issue_created_at,
           issue_updated_at,
           last_edited_at,
           issue_closed_at,
           namespace_id,
           visibility_level,

           {% for field in fields_to_mask %}
           CASE
             WHEN is_confidential = TRUE AND namespace_id NOT IN {{ get_internal_namespaces() }} THEN 'confidential - masked'
             WHEN visibility_level != 'public' AND namespace_id NOT IN {{ get_internal_namespaces() }} THEN 'private/internal - masked'
             ELSE {{field}}
           END                                                         AS issue_{{field}},
           {% endfor %}

           CASE
             WHEN namespace_id = 9970
               AND ARRAY_CONTAINS('community contribution'::variant, agg_label)
               THEN TRUE
             ELSE FALSE
           END AS is_community_contributor_related,

           CASE
             WHEN ARRAY_CONTAINS('s1'::variant, agg_label) THEN 'severity 1'
             WHEN ARRAY_CONTAINS('s2'::variant, agg_label) THEN 'severity 2'
             WHEN ARRAY_CONTAINS('s3'::variant, agg_label) THEN 'severity 3'
             WHEN ARRAY_CONTAINS('s4'::variant, agg_label) THEN 'severity 4'
             ELSE 'undefined'
           END as severity_tag,

           CASE
             WHEN ARRAY_CONTAINS('p1'::variant, agg_label) THEN 'priority 1'
             WHEN ARRAY_CONTAINS('p2'::variant, agg_label) THEN 'priority 2'
             WHEN ARRAY_CONTAINS('p3'::variant, agg_label) THEN 'priority 3'
             WHEN ARRAY_CONTAINS('p4'::variant, agg_label) THEN 'priority 4'
             ELSE 'undefined'
           END AS priority_tag,

           CASE WHEN namespace_id = 9970
             AND ARRAY_CONTAINS('security'::variant, agg_label) THEN TRUE
            ELSE FALSE
           END AS is_security_issue,

           CASE WHEN issues.project_id IN ({{is_project_included_in_engineering_metrics()}}) THEN TRUE
                ELSE FALSE END AS is_included_in_engineering_metrics,

           state,
           weight,
           due_date,
           lock_version,
           time_estimate,
           has_discussion_locked,
           ARRAY_TO_STRING(agg_label,'|') AS masked_label_title

    FROM issues
      LEFT JOIN agg_labels
        ON issues.issue_id = agg_labels.issue_id
      LEFT JOIN projects
        ON issues.project_id = projects.project_id
)

SELECT * from joined
