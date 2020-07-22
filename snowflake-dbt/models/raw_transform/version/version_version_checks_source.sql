{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}


WITH source AS (

    SELECT *
    FROM {{ source('version', 'version_checks') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})
    {% endif %}

), renamed AS (

    SELECT
      id::INTEGER                AS id,
      host_id::INTEGER           AS host_id,
      created_at::TIMESTAMP      AS created_at,
      updated_at::TIMESTAMP      AS updated_at,
      gitlab_version::VARCHAR    AS gitlab_version,
      referer_url::VARCHAR       AS referer_url,
      PARSE_JSON(request_data)   AS request_data
    FROM source

)

SELECT * 
FROM renamed
ORDER BY updated_at
