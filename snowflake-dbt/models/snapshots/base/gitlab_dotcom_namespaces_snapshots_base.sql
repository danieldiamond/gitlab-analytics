{{ config({
    "schema": "staging",
    "alias": "gitlab_dotcom_namespaces_snapshots"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('snapshots', 'gitlab_dotcom_namespaces_snapshots') }}
    
), renamed as (

  SELECT
  
    dbt_scd_id::VARCHAR                                           AS namespace_snapshot_id,
    id::INTEGER                                                   AS namespace_id,
    name::VARCHAR                                                 AS namespace_name,
    path::VARCHAR                                                 AS namespace_path,
    owner_id::INTEGER                                             AS owner_id,
    type                                                          AS namespace_type,
    IFF(avatar IS NULL, FALSE, TRUE)                              AS has_avatar,
    created_at::TIMESTAMP                                         AS namespace_created_at,
    updated_at::TIMESTAMP                                         AS namespace_updated_at,
    membership_lock::BOOLEAN                                      AS is_membership_locked,
    request_access_enabled::BOOLEAN                               AS has_request_access_enabled,
    share_with_group_lock::BOOLEAN                                AS has_share_with_group_locked,
    CASE
      WHEN visibility_level = '20' THEN 'public'
      WHEN visibility_level = '10' THEN 'internal'
      ELSE 'private'
    END                                                           AS visibility_level,
    ldap_sync_status,
    ldap_sync_error,
    ldap_sync_last_update_at::TIMESTAMP                           AS ldap_sync_last_update_at,
    ldap_sync_last_successful_update_at::TIMESTAMP                AS ldap_sync_last_successful_update_at,
    ldap_sync_last_sync_at::TIMESTAMP                             AS ldap_sync_last_sync_at,
    lfs_enabled::BOOLEAN                                          AS lfs_enabled,
    parent_id::INTEGER                                            AS parent_id,
    shared_runners_minutes_limit::NUMBER                          AS shared_runners_minutes_limit,
    repository_size_limit::NUMBER                                 AS repository_size_limit,
    require_two_factor_authentication::BOOLEAN                    AS does_require_two_factor_authentication,
    two_factor_grace_period::NUMBER                               AS two_factor_grace_period,
    plan_id::INTEGER                                              AS plan_id,
    project_creation_level::INTEGER                               AS project_creation_level,
    "DBT_VALID_FROM"::TIMESTAMP                                   AS valid_from,
    "DBT_VALID_TO"::TIMESTAMP                                     AS valid_to

  FROM source
    
)

SELECT *
FROM renamed
