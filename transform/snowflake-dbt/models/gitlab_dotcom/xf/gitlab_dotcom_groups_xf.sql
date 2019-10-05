{% set paid_plans = (2, 3, 4) %}
{% set fields_to_mask = ['group_name', 'group_path'] %}

WITH groups AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_groups')}}

),

members AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_members')}}

),

projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

), namespace_lineage AS (

    SELECT
      namespace_id,
      ultimate_parent_id,
      ( ultimate_parent_id IN {{ get_internal_parent_namespaces() }} )
          AS namespace_is_internal

    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

),

joined AS (

    SELECT
      groups.group_id,
      namespace_lineage.namespace_is_internal                           AS group_is_internal,

      {% for field in fields_to_mask %}
      CASE
        WHEN groups.visibility_level = 'public' OR namespace_is_internal THEN groups.{{field}}
        WHEN groups.visibility_level = 'internal' AND NOT namespace_is_internal THEN 'internal - masked'
        WHEN groups.visibility_level = 'private'  AND NOT namespace_is_internal THEN 'private - masked'
      END                                                               AS {{field}},
      {% endfor %}

      groups.owner_id,
      groups.has_avatar,
      groups.group_created_at,
      groups.group_updated_at,
      groups.is_membership_locked,
      groups.has_request_access_enabled,
      groups.has_share_with_group_locked,
      groups.visibility_level,
      groups.ldap_sync_status,
      groups.ldap_sync_error,
      groups.ldap_sync_last_update_at,
      groups.ldap_sync_last_successful_update_at,
      groups.ldap_sync_last_sync_at,
      groups.lfs_enabled,
      groups.parent_group_id,
      IFF(groups.parent_group_id IS NULL, True, False)                         AS is_top_level_group,
      namespace_lineage.ultimate_parent_id                              AS group_ultimate_parent_id,
      groups.shared_runners_minutes_limit,
      groups.repository_size_limit,
      groups.does_require_two_factor_authentication,
      groups.two_factor_grace_period,
      COALESCE(ultimate_parent_groups.plan_id, groups.plan_id)          AS plan_id,
      groups.project_creation_level,
      COALESCE((COALESCE(ultimate_parent_groups.plan_id,
                          groups.plan_id) IN {{ paid_plans }}) , False) AS group_plan_is_paid,
      COALESCE(COUNT(DISTINCT members.member_id), 0)                    AS member_count,
      COALESCE(COUNT(DISTINCT projects.project_id), 0)                  AS project_count

    FROM groups
      LEFT JOIN members
        ON groups.group_id = members.source_id
        AND members.member_source_type = 'Namespace'
      LEFT JOIN projects
        ON projects.namespace_id = groups.group_id
      LEFT JOIN namespace_lineage
        ON groups.group_id = namespace_lineage.namespace_id
      LEFT JOIN groups AS ultimate_parent_groups
        ON namespace_lineage.ultimate_parent_id = ultimate_parent_groups.group_id
    {{ dbt_utils.group_by(n=27) }}

)

SELECT *
FROM joined
