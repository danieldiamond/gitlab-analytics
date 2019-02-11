WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.users

), renamed AS (

    SELECT

      id :: integer                                                      as user_id,
      TRY_CAST(remember_created_at as timestamp)                         as remember_created_at,
      sign_in_count :: integer                                           as sign_in_count,
      TRY_CAST(current_sign_in_at as timestamp)                          as current_sign_in_at,
      TRY_CAST(last_sign_in_at as timestamp)                             as last_sign_in_at,
      -- current_sign_in_ip   // hidden for privacy
      -- last_sign_in_ip   // hidden for privacy
      created_at :: timestamp                                            as user_created_at,
      updated_at :: timestamp                                            as user_updated_at,
      admin :: boolean                                                   as is_admin,
      projects_limit :: integer                                          as projects_limit,
      failed_attempts :: integer                                         as failed_attempts,
      TRY_CAST(locked_at as timestamp)                                   as locked_at,
      IFF(lower(locked_at) = 'nan', FALSE, TRUE)                         as user_locked,
      can_create_group :: boolean                                        as has_create_group_permissions,
      can_create_team :: boolean                                         as has_create_team_permissions,
      state,
      color_scheme_id :: integer                                         as color_scheme_id,
      TRY_CAST(password_expires_at as timestamp)                         as password_expires_at,
      TRY_CAST(created_by_id as integer)                                 as created_by_id,
      TRY_CAST(last_credential_check_at as timestamp)                    as last_credential_check_at,
      IFF(lower(avatar) = 'nan', FALSE, TRUE)                            as has_avatar,
      TRY_CAST(confirmed_at as timestamp)                                as confirmed_at,
      TRY_CAST(confirmation_sent_at as timestamp)                        as confirmation_sent_at,
      -- unconfirmed_email // hidden for privacy
      hide_no_ssh_key :: boolean                                         as has_hide_no_ssh_key_enabled,
      -- website_url // hidden for privacy
      TRY_CAST(admin_email_unsubscribed_at as timestamp)                 as admin_email_unsubscribed_at,
      -- notification_email // hidden for privacy
      TRY_CAST(hide_no_password as boolean)                              as has_hide_no_password_enabled,
      TRY_CAST(password_automatically_set as boolean)                    as is_password_automatically_set,
      IFF(lower(location) = 'nan', NULL, location)                       as location,
      -- public_email // hidden for privacy
      TRY_CAST(dashboard as integer)                                     as dashboard,
      TRY_CAST(consumed_timestep as integer)                             as consumed_timestep,
      TRY_CAST(layout as integer)                                        as layout,
      hide_project_limit :: boolean                                      as has_hide_project_limit_enabled,
      -- note // hidden for privacy
      TRY_CAST(otp_grace_period_started_at as timestamp)                 as otp_grace_period_started_at,
      external :: boolean                                                as is_external_user,
      -- organization // hidden for privacy
      auditor :: boolean                                                 as auditor,
      require_two_factor_authentication_from_group :: boolean            as does_require_two_factor_authentication_from_group,
      two_factor_grace_period :: integer                                 as two_factor_grace_period,
      TRY_CAST(ghost as boolean)                                         as is_ghost,
      TRY_CAST(last_activity_on as timestamp)                            as last_activity_on,
      TRY_CAST(notified_of_own_activity as boolean)                      as is_notified_of_own_activity,
      TRY_CAST(support_bot as boolean)                                   as is_support_bot,
      IFF(lower(preferred_language) = 'nan', NULL, preferred_language)   as preferred_language,
      TRY_CAST(theme_id as integer)                                      as theme_id,
      TO_TIMESTAMP(cast(_updated_at as int))                             as users_last_updated_at


    FROM source


)

SELECT *
FROM renamed