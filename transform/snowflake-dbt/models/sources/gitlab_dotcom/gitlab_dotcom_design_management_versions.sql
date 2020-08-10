WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'design_management_versions') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                                 AS version_id,
      issue_id::NUMBER                           AS issue_id,
      created_at::TIMESTAMP                       AS created_at,
      author_id::NUMBER                          AS author_id
    FROM source

)

SELECT *
FROM renamed
