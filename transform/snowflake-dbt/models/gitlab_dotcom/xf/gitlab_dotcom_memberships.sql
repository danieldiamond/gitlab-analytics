WITH members AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_members')}}
    WHERE is_currently_valid = True

), 

project_group_links AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_project_group_links')}}
    WHERE is_currently_valid = True

), 

namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

),

projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

),

project_group_links_unnested AS ( -- Where groups are invited to projects.

  SELECT
    namespace_lineage.ultimate_parent_id, -- Ultimate parent group of the project.
    project_group_links.project_id
    project_group_links.group_access,
    members.user_id
  FROM project_group_links
    INNER JOIN members
      ON project_group_links.project_id = members.source_id
      AND members.member_source_type = 'Project'
    INNER JOIN projects
      ON project_group_links.project_id = projects.project_id
    LEFT JOIN namespace_lineage
      ON projects.namespace_id = namespace_lineage.namespace_id

),

unioned AS (

  SELECT *
  FROM members

)

SELECT * FROM project_group_links_unnested
