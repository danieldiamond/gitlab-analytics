{% set fields_to_mask = ['milestone_title', 'milestone_description'] %}

WITH milestones AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_milestones')}}

),

-- Projects table joins milestones to namespaces
projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}
),

internal_namespaces AS (

    SELECT
      namespace_id
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}
    WHERE ultimate_parent_id IN {{ get_internal_parent_namespaces() }}
),


SELECT
    milestones.milestone_id,

   {% for field in fields_to_mask %}
    CASE
      WHEN is_confidential = TRUE
        AND internal_namespaces.namespace_id IS NULL
        THEN 'confidential - masked'
      WHEN visibility_level != 'public'
        AND internal_namespaces.namespace_id IS NULL
        THEN 'private/internal - masked'
      ELSE {{field}}
    END                                          AS {{field}},
    {% endfor %},

    milestones.project_id,
    milestones.group_id,
    milestones.start_date,
    milestones.due_date,
    milestones.milestone_status,
    milestones.milestone_created_at,
    milestones.milestone_updated_at
    
FROM milestones
  INNER JOIN projects
    ON milestones.project_id = projects.project_id
  LEFT JOIN internal_namespaces
    ON projects.namespaces = internal_namespaces.namespace_id
