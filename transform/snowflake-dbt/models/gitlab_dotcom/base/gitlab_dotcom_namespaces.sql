{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'namespaces') }}

), gitlab_subscriptions AS (

    /*
      Here we only want the current (most recent) gitlab subscription for each namespace.
      This windowing approach does not handle hard deletes correctly. 
      We will develop a more elegant approach after https://gitlab.com/gitlab-data/analytics/issues/2727
    */
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY namespace_id
        ORDER BY gitlab_subscription_updated_at DESC
      ) AS rank_in_namespace_id
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}

), namespaces AS (

    SELECT 
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
      project_creation_level::INTEGER                               AS project_creation_level
    FROM source
    WHERE rank_in_key = 1

), joined AS (

  SELECT
    namespaces.*,
    COALESCE(gitlab_subscriptions.plan_id, 34) AS plan_id
  FROM namespaces
    LEFT JOIN gitlab_subscriptions
      ON namespaces.namespace_id = gitlab_subscriptions.namespace_id
      AND gitlab_subscriptions.rank_in_namespace_id = 1

)

SELECT *
FROM joined
