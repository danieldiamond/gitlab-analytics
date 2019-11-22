WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'members') }}

), renamed AS (

    SELECT DISTINCT

      id::INTEGER                                    AS member_id,
      access_level::INTEGER                          AS access_level,
      source_id::INTEGER                             AS source_id,
      source_type                                    AS member_source_type,
      user_id::INTEGER                               AS user_id,
      notification_level::INTEGER                    AS notification_level,
      type                                           AS member_type,
      created_at::TIMESTAMP                          AS invite_created_at,
      created_by_id::INTEGER                         AS created_by_id,
      invite_accepted_at::TIMESTAMP                  AS invite_accepted_at,
      requested_at::TIMESTAMP                        AS requested_at,
      expires_at::TIMESTAMP                          AS expires_at,
      ldap::BOOLEAN                                  AS has_ldap,
      override::BOOLEAN                              AS has_override

    FROM source
    WHERE _task_instance IN (SELECT MAX(_task_instance) FROM source)

)

{{ scd_type_6(
    primary_key='member_id',
    source_cte='source',
    casted_cte='renamed'
) }}
