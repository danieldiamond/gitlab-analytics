{% set fields_to_mask = ['milestone_title', 'milestone_description'] %}

WITH milestones AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_milestones')}}

),

-- A milestone joins to a namespace through EITHER a project or group
projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}
),

internal_namespaces AS (

    SELECT
      namespace_id
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}
    WHERE ultimate_parent_id IN {{ get_internal_parent_namespaces() }}
)


SELECT
    milestones.milestone_id,

    {% for field in fields_to_mask %}
    IFF(internal_namespaces.namespace_id IS NULL,
        'private - masked', {{field}})                     AS {{field}},
    {% endfor %}

    milestones.project_id,
    milestones.group_id,
    milestones.start_date,
    milestones.due_date,
    milestones.milestone_status,
    milestones.milestone_created_at,
    milestones.milestone_updated_at,
    COALESCE(milestones.group_id, projects.namespace_id)   AS namespace_id

FROM milestones
  LEFT JOIN projects
    ON milestones.project_id = projects.project_id
  LEFT JOIN internal_namespaces
    ON projects.namespace_id = internal_namespaces.namespace_id
    OR milestones.group_id = internal_namespaces.namespace_id
