{% set fields_to_mask = ['name', 'path'] %}

WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.namespaces

), renamed AS (

    SELECT

      id :: integer                                                   as namespace_id,
      {% for field in fields_to_mask %}
      CASE
        WHEN visibility_level = '20' THEN {{field}}
        WHEN visibility_level = '10' THEN 'internal - masked'
        ELSE 'private - masked'
      END                                                             as namespace_{{field}},
      {% endfor %}
      owner_id :: integer                                             as owner_id,
      type,
      IFF(avatar IS NULL, FALSE, TRUE)                                as has_avatar,
      created_at :: timestamp                                         as namespace_created_at,
      updated_at :: timestamp                                         as namespace_updated_at,
      membership_lock :: boolean                                      as is_membership_locked,
      request_access_enabled :: boolean                               as has_request_access_enabled,
      share_with_group_lock :: boolean                                as has_share_with_group_locked,
      CASE
        WHEN visibility_level = '20' THEN 'public'
        WHEN visibility_level = '10' THEN 'internal'
        ELSE 'private'
      END                                                             as visibiity_level,
      ldap_sync_status,
      ldap_sync_error,
      TRY_CAST(ldap_sync_last_update_at as timestamp)                 as ldap_sync_last_update_at,
      TRY_CAST(ldap_sync_last_successful_update_at as timestamp)      as ldap_sync_last_successful_update_at,
      TRY_CAST(ldap_sync_last_sync_at as timestamp)                   as ldap_sync_last_sync_at,
      TRY_CAST(lfs_enabled as boolean)                                as lfs_enabled,
      TRY_CAST(parent_id as integer)                                  as parent_id,
      shared_runners_minutes_limit :: number                          as shared_runners_minutes_limit,
      TRY_CAST(repository_size_limit as number)                       as repository_size_limit,
      require_two_factor_authentication :: boolean                    as does_require_two_factor_authentication,
      two_factor_grace_period :: number                               as two_factor_grace_period,
      plan_id :: integer                                              as plan_id,
      TRY_CAST(project_creation_level as integer)                     as project_creation_level

    FROM source


)

SELECT *
FROM renamed