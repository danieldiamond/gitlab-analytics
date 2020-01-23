WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'design_management_designs') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      MD5(id)::VARCHAR                            AS design_management_version_id,
      issue_id::INTEGER                           AS issue_id,
      created_at::TIMESTAMP                       AS created_at,
      author_id::INTEGER                          AS author_id
    FROM source

)

SELECT *
FROM renamed
