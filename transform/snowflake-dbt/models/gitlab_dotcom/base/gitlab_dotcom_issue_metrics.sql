{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *,
					ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'issue_metrics') }}

), renamed AS (

    SELECT

      id :: integer                                               as issue_metric_id,
      issue_id :: integer                                         as issue_id,
      first_mentioned_in_commit_at::date                          as first_mentioned_in_commit_at,
      first_associated_with_milestone_at::date                    as first_associated_with_milestone_at,
      first_added_to_board_at::date                               as first_added_to_board_at,
      created_at :: timestamp                                     as issue_metric_created_at,
      updated_at :: timestamp                                     as issue_metric_updated_at


    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
