WITH source AS (

      SELECT *
      FROM {{ source('gitlab_dotcom', 'lists') }}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

      SELECT
        id::INTEGER               AS list_id
        board_id::INTEGER         AS board_id
        label_id::INTEGER         AS label_id
        list_type::INTEGER        AS list_type
        created_at::TIMESTAMP     AS created_at
        updated_at::TIMESTAMP     AS updated_at
        user_id::INTEGER          AS user_id
        milestone_id::INTEGER     AS milestone_id
        max_issue_count::INTEGER  AS max_issue_count
        max_issue_weight::INTEGER AS max_issue_weight
        limit_metric::VARCHAR     AS limit_metric
      FROM source

)

SELECT *
FROM renamed
