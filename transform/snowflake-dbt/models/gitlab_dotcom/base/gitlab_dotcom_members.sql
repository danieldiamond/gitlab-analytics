-- disabled model until the data starts flowing in (the source table is missing from tap_postgres)

WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'members') }}

), renamed AS (

    SELECT

      id :: integer                                    as member_id,
      access_level :: integer                          as access_level,
      source_id :: integer                             as source_id,
      source_type                                      as member_source_type,
      user_id :: integer                               as user_id,
      notification_level :: integer                    as notification_level,
      type                                             as member_type,
      created_by_id :: integer                         as created_by_id,
      invite_accepted_at :: timestamp                  as invite_accepted_at,
      requested_at :: timestamp                        as requested_at,
      expires_at :: timestamp                          as expires_at,
      ldap :: boolean                                  as has_ldap,
      override :: boolean                              as has_override

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
