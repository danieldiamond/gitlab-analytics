{{ config({
    "materialized": "incremental",
    "unique_key": "user_id"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'users') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})
    {% endif %}

), renamed AS (

    SELECT
      id::INTEGER                                                      AS user_id,
      remember_created_at::TIMESTAMP                                   AS remember_created_at,
      sign_in_count::INTEGER                                           AS sign_in_count,
      current_sign_in_at::TIMESTAMP                                    AS current_sign_in_at,
      last_sign_in_at::TIMESTAMP                                       AS last_sign_in_at,
      -- current_sign_in_ip   // hidden for privacy
      -- last_sign_in_ip   // hidden for privacy
      created_at::TIMESTAMP                                            AS created_at,
      updated_at::TIMESTAMP                                            AS updated_at,
      admin::BOOLEAN                                                   AS is_admin,
      projects_limit::INTEGER                                          AS projects_limit,
      failed_attempts::INTEGER                                         AS failed_attempts,
      locked_at::TIMESTAMP                                             AS locked_at,
      IFF(lower(locked_at) = 'nan', FALSE, TRUE)                       AS user_locked,
      can_create_group::BOOLEAN                                        AS has_create_group_permissions,
      can_create_team::BOOLEAN                                         AS has_create_team_permissions,
      state,
      color_scheme_id::INTEGER                                         AS color_scheme_id,
      password_expires_at::TIMESTAMP                                   AS password_expires_at,
      created_by_id::INTEGER                                           AS created_by_id,
      last_credential_check_at::TIMESTAMP                              AS last_credential_check_at,
      IFF(lower(avatar) = 'nan', FALSE, TRUE)                          AS has_avatar,
      confirmed_at::TIMESTAMP                                          AS confirmed_at,
      confirmation_sent_at::TIMESTAMP                                  AS confirmation_sent_at,
      -- unconfirmed_email // hidden for privacy
      hide_no_ssh_key::BOOLEAN                                         AS has_hide_no_ssh_key_enabled,
      -- website_url // hidden for privacy
      admin_email_unsubscribed_at::TIMESTAMP                           AS admin_email_unsubscribed_at,
      -- notification_email // hidden for privacy
      hide_no_password::BOOLEAN                                        AS has_hide_no_password_enabled,
      password_automatically_set::BOOLEAN                              AS is_password_automatically_set,
      IFF(lower(location) = 'nan', NULL, location)                     AS location,
      -- public_email // hidden for privacy
      dashboard::INTEGER                                               AS dashboard,
      consumed_timestep::INTEGER                                       AS consumed_timestep,
      layout::INTEGER                                                  AS layout,
      hide_project_limit::BOOLEAN                                      AS has_hide_project_limit_enabled,
      -- note // hidden for privacy
      otp_grace_period_started_at::TIMESTAMP                           AS otp_grace_period_started_at,
      external::BOOLEAN                                                AS is_external_user,
      organization                                                     AS organization,
      auditor::BOOLEAN                                                 AS auditor,
      require_two_factor_authentication_from_group::BOOLEAN            AS does_require_two_factor_authentication_from_group,
      two_factor_grace_period::INTEGER                                 AS two_factor_grace_period,
      ghost::BOOLEAN                                                   AS is_ghost,
      last_activity_on::TIMESTAMP                                      AS last_activity_on,
      notified_of_own_activity::BOOLEAN                                AS is_notified_of_own_activity,
      NULLIF(preferred_language, 'nan')::VARCHAR                       AS preferred_language,
      theme_id::INTEGER                                                AS theme_id,
      accepted_term_id::INTEGER                                        AS accepted_term_id,
      private_profile::BOOLEAN                                         AS is_private_profile,
      roadmap_layout::INTEGER                                          AS roadmap_layout,
      include_private_contributions::BOOLEAN                           AS include_private_contributions,
      group_view::INTEGER                                              AS group_views,
      managing_group_id::INTEGER                                       AS managing_group_id,
      bot_type::INTEGER                                                AS bot_type,
      source.role::INTEGER                                             AS role_id,
      {{user_role_mapping(user_role='source.role')}}::VARCHAR          AS role

    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1

)

SELECT  *
FROM renamed
ORDER BY updated_at
