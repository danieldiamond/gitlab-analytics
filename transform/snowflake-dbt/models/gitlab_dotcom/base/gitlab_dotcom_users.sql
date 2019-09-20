{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'users') }}

), renamed AS (

    SELECT

      id::INTEGER                                                      AS user_id,
      remember_created_at :: timestamp                                   AS remember_created_at,
      sign_in_count::INTEGER                                           AS sign_in_count,
      current_sign_in_at :: timestamp                                    AS current_sign_in_at,
      last_sign_in_at :: timestamp                                       AS last_sign_in_at,
      -- current_sign_in_ip   // hidden for privacy
      -- last_sign_in_ip   // hidden for privacy
      created_at :: timestamp                                            AS user_created_at,
      updated_at :: timestamp                                            AS user_updated_at,
      admin :: boolean                                                   AS is_admin,
      projects_limit::INTEGER                                          AS projects_limit,
      failed_attempts::INTEGER                                         AS failed_attempts,
      locked_at :: timestamp                                             AS locked_at,
      IFF(lower(locked_at) = 'nan', FALSE, TRUE)                         AS user_locked,
      can_create_group :: boolean                                        AS has_create_group_permissions,
      can_create_team :: boolean                                         AS has_create_team_permissions,
      state,
      color_scheme_id::INTEGER                                         AS color_scheme_id,
      password_expires_at :: timestamp                                   AS password_expires_at,
      created_by_id::INTEGER                                           AS created_by_id,
      last_credential_check_at :: timestamp                              AS last_credential_check_at,
      IFF(lower(avatar) = 'nan', FALSE, TRUE)                            AS has_avatar,
      confirmed_at :: timestamp                                          AS confirmed_at,
      confirmation_sent_at :: timestamp                                  AS confirmation_sent_at,
      -- unconfirmed_email // hidden for privacy
      hide_no_ssh_key :: boolean                                         AS has_hide_no_ssh_key_enabled,
      -- website_url // hidden for privacy
      admin_email_unsubscribed_at :: timestamp                           AS admin_email_unsubscribed_at,
      -- notification_email // hidden for privacy
      hide_no_password :: boolean                                        AS has_hide_no_password_enabled,
      password_automatically_set :: boolean                              AS is_password_automatically_set,
      IFF(lower(location) = 'nan', NULL, location)                       AS location,
      -- public_email // hidden for privacy
      dashboard::INTEGER                                               AS dashboard,
      consumed_timestep::INTEGER                                       AS consumed_timestep,
      layout::INTEGER                                                  AS layout,
      hide_project_limit :: boolean                                      AS has_hide_project_limit_enabled,
      -- note // hidden for privacy
      otp_grace_period_started_at :: timestamp                           AS otp_grace_period_started_at,
      external :: boolean                                                AS is_external_user,
      organization                                                       AS organization,
      auditor :: boolean                                                 AS auditor,
      require_two_factor_authentication_from_group :: boolean            AS does_require_two_factor_authentication_from_group,
      two_factor_grace_period::INTEGER                                 AS two_factor_grace_period,
      ghost :: boolean                                                   AS is_ghost,
      last_activity_on :: timestamp                                      AS last_activity_on,
      notified_of_own_activity :: boolean                                AS is_notified_of_own_activity,
      IFF(lower(preferred_language) = 'nan', NULL, preferred_language)   AS preferred_language,
      theme_id::INTEGER                                                AS theme_id

    FROM source
    WHERE rank_in_key = 1

)

SELECT  *
FROM renamed
