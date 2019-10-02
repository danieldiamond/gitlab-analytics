{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'issue_metrics') }}

), renamed AS (

    SELECT

      id::INTEGER                                               AS issue_metric_id,
      issue_id::INTEGER                                         AS issue_id,
      first_mentioned_in_commit_at::DATE                          AS first_mentioned_in_commit_at,
      first_associated_with_milestone_at::DATE                    AS first_associated_with_milestone_at,
      first_added_to_board_at::DATE                               AS first_added_to_board_at,
      created_at::TIMESTAMP                                     AS issue_metric_created_at,
      updated_at::TIMESTAMP                                     AS issue_metric_updated_at


    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
