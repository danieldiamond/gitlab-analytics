WITH members AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_members')}}

), project_group_links AS (

  SELECT *
  FROM {{ref('gitlab_dotcom_members')}}

), project_group_links_unnested AS (

  SELECT
    project_group_links.project_id,
    memmbers.user_id
  FROM project_group_links
    INNER JOIN members
      ON project_group_links.project_id = members.source_id
      AND members.member_source_type = 'Project'

)

SELECT * FROM project_group_links_unnested