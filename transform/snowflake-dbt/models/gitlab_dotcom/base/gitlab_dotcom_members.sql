WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.members

), renamed AS (

    SELECT

      id :: integer                                    as member_id,
      TRY_CAST(access_level as integer)                as access_level,
      TRY_CAST(source_id as integer)                   as source_id,
      source_type                                      as member_source_type,
      TRY_CAST(user_id as integer)                     as user_id,
      TRY_CAST(notification_level as integer)          as notification_level,
      type                                             as member_type,
      TRY_CAST(created_by_id as integer)               as created_by_id,
      TRY_CAST(invite_accepted_at as timestamp)        as invite_accepted_at,
      TRY_CAST(requested_at as timestamp)              as requested_at,
      TRY_CAST(expires_at as timestamp)                as expires_at,
      ldap :: boolean                                  as has_ldap,
      override :: boolean                              as has_override,
      TO_TIMESTAMP(_updated_at :: integer)             as members_last_updated_at

    FROM source


)

SELECT *
FROM renamed