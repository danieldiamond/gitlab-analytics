WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.issue_metrics

), renamed AS (

    SELECT

      id :: integer                                               as issue_metrics_id,
      issue_id :: integer                                         as issue_id,
      TRY_CAST(first_mentioned_in_commit_at as timestamp)         as issue_first_mentioned_in_commit_at,
      TRY_CAST(first_associated_with_milestone_at as timestamp)   as issue_first_associated_with_milestone_at,
      TRY_CAST(first_added_to_board_at as timestamp)              as issue_first_added_to_board_at,
      created_at :: timestamp                                     as issue_metrics_created_at,
      updated_at :: timestamp                                     as issue_metrics_updated_at


    FROM source


)

SELECT *
FROM renamed