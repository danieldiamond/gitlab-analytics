{% set fields_to_mask = ['title', 'description'] %}

with issues as (

    SELECT * FROM {{ref('gitlab_dotcom_issues')}}

), all_label_links as (

    SELECT * FROM {{ref('gitlab_dotcom_label_links')}}

), label_links as (

    SELECT * FROM all_label_links where target_type = 'Issue'

), all_labels as (

    SELECT * FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels as (

    SELECT
      issue_id,
      ARRAY_AGG(lower(masked_label_title)) WITHIN GROUP (ORDER BY issue_id asc) as agg_label

    FROM issues
      LEFT JOIN label_links on issues.issue_id = label_links.target_id
      LEFT JOIN all_labels on label_links.label_id = all_labels.label_id
    GROUP BY issue_id

), all_projects as (

    SELECT * from {{ref('gitlab_dotcom_projects')}}

), gitlab_org_projects as (

    SELECT project_id FROM all_projects where namespace_id = 9970

), private_projects AS (

  SELECT
     project_id,
    'not-public' as visibility
  FROM all_projects
  WHERE visibility_level != 'public'

),  joined as (

    SELECT
      issues.issue_id,
      author_id,
      issues.project_id,
      milestone_id,
      updated_by_id,
      last_edited_by_id,
      moved_to_id,
      issue_created_at,
      issue_updated_at,
      last_edited_at,
      closed_at,
      is_confidential,

      {% for field in fields_to_mask %}
      CASE
        WHEN is_confidential = TRUE THEN 'confidential issue - content masked'
        WHEN private_projects.visibility = 'not-public' THEN 'private/internal project - content masked'
        ELSE {{field}}
      END                                                         as issue_{{field}},
      {% endfor %}

      CASE
        WHEN issues.project_id IN (select * from gitlab_org_projects)
          AND ARRAY_CONTAINS('community contribution'::variant, agg_label)
          THEN TRUE
        ELSE FALSE
      END as is_community_contributor_related,

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
      END as priority_tag,

      state,
      weight,
      due_date,
      lock_version,
      time_estimate,
      has_discussion_locked,
      issues_last_updated_at,
      ARRAY_TO_STRING(agg_label,'|') as masked_label_title

    FROM issues
      LEFT JOIN agg_labels on issues.issue_id = agg_labels.issue_id
      LEFT JOIN private_projects on issues.project_id = private_projects.project_id
)

SELECT * from joined