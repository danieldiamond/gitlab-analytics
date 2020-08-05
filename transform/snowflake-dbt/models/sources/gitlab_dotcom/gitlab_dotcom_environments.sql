{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'environments') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                                      AS environment_id,
      project_id::INTEGER                              AS project_id,
      name::VARCHAR                                    AS environment_name,
      created_at::TIMESTAMP                            AS created_at,
      updated_at::TIMESTAMP                            AS updated_at,
      external_url::VARCHAR                            AS external_url,
      environment_type::VARCHAR                        AS environment_type,
      state::VARCHAR                                   AS state,
      slug::VARCHAR                                    AS slug
    FROM source

)
SELECT *
FROM renamed
