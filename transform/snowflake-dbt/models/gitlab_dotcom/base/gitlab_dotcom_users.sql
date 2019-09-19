{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'users') }}

), renamed AS (

    SELECT

      id :: integer                                                      as user_id,
      remember_created_at :: timestamp                                   as remember_created_at,
      sign_in_count :: integer                                           as sign_in_count,
      current_sign_in_at :: timestamp                                    as current_sign_in_at,
      last_sign_in_at :: timestamp                                       as last_sign_in_at,
      -- current_sign_in_ip   // hidden for privacy
      -- last_sign_in_ip   // hidden for privacy
      created_at :: timestamp                                            as user_created_at,
      updated_at :: timestamp                                            as user_updated_at,
      admin :: boolean                                                   as is_admin,
      projects_limit :: integer                                          as projects_limit,
      failed_attempts :: integer                                         as failed_attempts,
      locked_at :: timestamp                                             as locked_at,
      IFF(lower(locked_at) = 'nan', FALSE, TRUE)                         as user_locked,
      can_create_group :: boolean                                        as has_create_group_permissions,
      can_create_team :: boolean                                         as has_create_team_permissions,
      state,
      color_scheme_id :: integer                                         as color_scheme_id,
      password_expires_at :: timestamp                                   as password_expires_at,
      created_by_id :: integer                                           as created_by_id,
      last_credential_check_at :: timestamp                              as last_credential_check_at,
      IFF(lower(avatar) = 'nan', FALSE, TRUE)                            as has_avatar,
      confirmed_at :: timestamp                                          as confirmed_at,
      confirmation_sent_at :: timestamp                                  as confirmation_sent_at,
      -- unconfirmed_email // hidden for privacy
      hide_no_ssh_key :: boolean                                         as has_hide_no_ssh_key_enabled,
      -- website_url // hidden for privacy
      admin_email_unsubscribed_at :: timestamp                           as admin_email_unsubscribed_at,
      -- notification_email // hidden for privacy
      hide_no_password :: boolean                                        as has_hide_no_password_enabled,
      password_automatically_set :: boolean                              as is_password_automatically_set,
      IFF(lower(location) = 'nan', NULL, location)                       as location,
      -- public_email // hidden for privacy
      dashboard :: integer                                               as dashboard,
      consumed_timestep :: integer                                       as consumed_timestep,
      layout :: integer                                                  as layout,
      hide_project_limit :: boolean                                      as has_hide_project_limit_enabled,
      -- note // hidden for privacy
      otp_grace_period_started_at :: timestamp                           as otp_grace_period_started_at,
      external :: boolean                                                as is_external_user,
      organization                                                       as organization,
      auditor :: boolean                                                 as auditor,
      require_two_factor_authentication_from_group :: boolean            as does_require_two_factor_authentication_from_group,
      two_factor_grace_period :: integer                                 as two_factor_grace_period,
      ghost :: boolean                                                   as is_ghost,
      last_activity_on :: timestamp                                      as last_activity_on,
      notified_of_own_activity :: boolean                                as is_notified_of_own_activity,
      IFF(lower(preferred_language) = 'nan', NULL, preferred_language)   as preferred_language,
      theme_id :: integer                                                as theme_id

    FROM source
    WHERE rank_in_key = 1

)

SELECT  *
FROM renamed
