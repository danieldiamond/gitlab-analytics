{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

{% set fields_to_mask = ['name', 'path'] %}
{% set gitlab_namespaces = (6543,9970,4347861) %}

with source as (

  SELECT *
  FROM {{ var("database") }}.gitlab_dotcom.namespaces

), renamed as (

    SELECT  id :: integer                                                   AS namespace_id,

            {% for field in fields_to_mask %}
            CASE
              WHEN visibility_level = '20' OR namespace_id::int IN {{gitlab_namespaces}} THEN {{field}}
              WHEN visibility_level = '10' AND namespace_id::int NOT IN {{gitlab_namespaces}} THEN 'internal - masked'
              WHEN visibility_level = '0' AND namespace_id::int NOT IN {{gitlab_namespaces}} THEN 'private - masked'
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
            TRY_CAST(ldap_sync_last_update_at AS timestamp)                 AS ldap_sync_last_update_at,
            TRY_CAST(ldap_sync_last_successful_update_at AS timestamp)      AS ldap_sync_last_successful_update_at,
            TRY_CAST(ldap_sync_last_sync_at AS timestamp)                   AS ldap_sync_last_sync_at,
            TRY_CAST(lfs_enabled AS boolean)                                AS lfs_enabled,
            TRY_CAST(parent_id AS integer)                                  AS parent_id,
            shared_runners_minutes_limit :: number                          AS shared_runners_minutes_limit,
            TRY_CAST(repository_size_limit AS number)                       AS repository_size_limit,
            require_two_factor_authentication :: boolean                    AS does_require_two_factor_authentication,
            two_factor_grace_period :: number                               AS two_factor_grace_period,
            plan_id :: integer                                              AS plan_id,
            TRY_CAST(project_creation_level as integer)                     AS project_creation_level

    FROM source


)

SELECT *
FROM renamed