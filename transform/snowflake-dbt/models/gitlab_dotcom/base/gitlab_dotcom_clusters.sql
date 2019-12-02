{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'clusters') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
        id::INTEGER                AS cluster_id,
        user_id::INTEGER           AS user_id,
        provider_type::VARCHAR     AS provider_type,
        platform_type::VARCHAR     AS platform_type,
        created_at::TIMESTAMP      AS created_at,
        updated_at::TIMESTAMP      AS updated_at,
        enabled::BOOLEAN           AS is_enabled,
        name::VARCHAR              AS cluster_name,
        environment_scope::VARCHAR AS environment_scope,
        cluster_type::VARCHAR      AS cluster_type,
        domain::VARCHAR            AS domain,
        managed::VARCHAR           AS managed

    FROM source

)


SELECT *
FROM renamed
