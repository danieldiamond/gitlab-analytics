WITH members AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_members')}}
    WHERE is_currently_valid = True
      AND user_id IS NOT NULL
    QUALIFY RANK() OVER (
      PARTITION BY
        user_id,
        source_id,
        member_source_type
      ORDER BY
        access_level DESC,
        invite_created_at DESC
    ) = 1

),

group_group_links AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_group_group_links')}}
    WHERE is_currently_valid = True

),

project_group_links AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_project_group_links')}}
    WHERE is_currently_valid = True

),

namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces_xf')}}

),

projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

),

group_members AS (

    SELECT *
    FROM members
    WHERE member_source_type = 'Namespace'

),

project_members AS (

    SELECT
      projects.namespace_id,
      members.*
    FROM members
      INNER JOIN projects
        ON members.source_id = projects.project_id
    WHERE member_source_type = 'Project'

),

group_group_links_unnested AS ( -- Where groups are invited to groups.

    SELECT
      group_group_links.shared_group_id, -- The "host" group.
      group_group_links.group_group_link_id,
      group_group_links.shared_with_group_id, -- The "guest" group.
      group_group_links.group_access AS access_level,
      group_members.user_id
    FROM group_group_links
      INNER JOIN group_members
        ON group_group_links.shared_with_group_id = group_members.source_id

),

project_group_links_unnested AS ( -- Where groups are invited to projects.

    SELECT
      projects.namespace_id, -- The group that the "host" project directly belongs to.
      project_group_links.project_group_link_id,
      project_group_links.project_id, -- The "host" project.
      project_group_links.group_access AS access_level,
      group_members.user_id
    FROM project_group_links
      INNER JOIN group_members
        ON project_group_links.group_id = group_members.source_id
      INNER JOIN projects
        ON project_group_links.project_id = projects.project_id

),

individual_namespaces AS (

  SELECT
    namespace_id,
    50        AS access_level, --implied
    owner_id  AS user_id
  FROM namespaces
  WHERE namespace_type = 'Individual'

),

unioned AS (

    SELECT
      source_id          AS namespace_id,
      user_id,
      access_level,
      'group_membership' AS membership_source_type,
      source_id          AS membership_source_id
    FROM group_members

    UNION

    SELECT
      namespace_id,
      user_id,
      access_level,
      'project_membership' AS membership_source_type,
      source_id            AS membership_source_id
    FROM project_members

    UNION

    SELECT
      shared_group_id       AS namespace_id,
      user_id,
      access_level,
      'group_group_link'    AS membership_source_type,
      group_group_link_id   AS membership_source_id
    FROM group_group_links_unnested

    UNION

    SELECT
      namespace_id,
      user_id,
      access_level,
      'project_group_link'  AS membership_source_type,
      project_group_link_id AS membership_source_id
    FROM project_group_links_unnested

    UNION

    SELECT
      namespace_id,
      user_id,
      access_level,
      'individual_namespace'   AS membership_source_type,
      namespace_id             AS membership_source_id
    FROM individual_namespaces

),

final AS ( -- Get ultimate parent of the namespace.

    SELECT
      namespaces.namespace_ultimate_parent_id AS ultimate_parent_id,
      namespaces.plan_id                      AS ultimate_parent_plan_id,
      unioned.*
    FROM unioned
      INNER JOIN namespaces
        ON unioned.namespace_id = namespaces.namespace_id

)

SELECT *
FROM final
