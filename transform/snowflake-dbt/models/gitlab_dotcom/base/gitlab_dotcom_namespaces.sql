{% set fields_to_mask = ['name', 'path'] %}

with source as (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'namespaces') }}

), renamed as (

    SELECT  id :: integer                                                   AS namespace_id,

            {% for field in fields_to_mask %}
            CASE
              WHEN visibility_level = '20' OR namespace_id::int IN {{ get_internal_namespaces() }} THEN {{field}}
              WHEN visibility_level = '10' AND namespace_id::int NOT IN {{ get_internal_namespaces() }} THEN 'internal - masked'
              WHEN visibility_level = '0' AND namespace_id::int NOT IN {{ get_internal_namespaces() }} THEN 'private - masked'
            END                                                             AS namespace_{{field}},
            {% endfor %}

            owner_id :: integer                                             AS owner_id,
            type                                                            AS namespace_type,
            IFF(avatar IS NULL, FALSE, TRUE)                                AS has_avatar,
            created_at :: timestamp                                         AS namespace_created_at,
            updated_at :: timestamp                                         AS namespace_updated_at,
            membership_lock :: boolean                                      AS is_membership_locked,
            request_access_enabled :: boolean                               AS has_request_access_enabled,
            share_with_group_lock :: boolean                                AS has_share_with_group_locked,
            CASE
              WHEN visibility_level = '20' THEN 'public'
              WHEN visibility_level = '10' THEN 'internal'
              ELSE 'private'
            END                                                             AS visibility_level,
            ldap_sync_status,
            ldap_sync_error,
            ldap_sync_last_update_at::timestamp                             AS ldap_sync_last_update_at,
            ldap_sync_last_successful_update_at::timestamp                  AS ldap_sync_last_successful_update_at,
            ldap_sync_last_sync_at::timestamp                               AS ldap_sync_last_sync_at,
            lfs_enabled::boolean                                            AS lfs_enabled,
            parent_id::integer                                              AS parent_id,
            shared_runners_minutes_limit :: number                          AS shared_runners_minutes_limit,
            repository_size_limit::number                                   AS repository_size_limit,
            require_two_factor_authentication :: boolean                    AS does_require_two_factor_authentication,
            two_factor_grace_period :: number                               AS two_factor_grace_period,
            plan_id :: integer                                              AS plan_id,
            project_creation_level::integer                                 AS project_creation_level,
            (namespace_id IN {{ get_internal_namespaces() }} )              AS namespace_is_internal

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
