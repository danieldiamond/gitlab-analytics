WITH source AS (

    SELECT *
    FROM {{ source('version', 'current_host_stats') }}

), renamed AS (

    SELECT
      id::INTEGER                         AS host_stat_id,
      host_id::INTEGER                    AS host_id,
      url::VARCHAR                        AS host_url,
      created_at::TIMESTAMP               AS created_at,
      updated_at::TIMESTAMP               AS updated_at,
      active_users_count::INTEGER         AS active_users_count,
      edition::VARCHAR                    AS edition,
      historical_max_users_count::INTEGER AS historical_max_users_count,
      host_created_at::TIMESTAMP          AS host_created_at,
      license_md5::VARCHAR                AS license_md5,
      usage_data_recorded_at::TIMESTAMP   AS usage_data_recorded_at,
      user_count::INTEGER                 AS user_count,
      version::VARCHAR                    AS version,
      version_check_created_at::TIMESTAMP AS version_check_created_at
    FROM source  

)

SELECT *
FROM renamed