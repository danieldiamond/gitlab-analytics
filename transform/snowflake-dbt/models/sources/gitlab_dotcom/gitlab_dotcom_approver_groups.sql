WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'approver_groups') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

  SELECT
    id::NUMBER                   AS approver_group_id,
    target_type::VARCHAR          AS target_type,
    group_id::NUMBER             AS group_id,
    created_at::TIMESTAMP         AS created_at,
    updated_at::TIMESTAMP         AS updated_at

  FROM source

)


SELECT *
FROM renamed
