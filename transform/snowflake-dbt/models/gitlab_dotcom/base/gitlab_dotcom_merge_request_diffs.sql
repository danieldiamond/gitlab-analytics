WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.merge_request_diffs


), renamed AS (

    SELECT

      id :: integer                                 as merge_request_diff_id,
      state                                         as merge_request_diff_status,
      merge_request_id :: integer                   as merge_request_id,
      real_size                                     as merge_request_real_size,
      TRY_CAST(commits_count as integer)            as commits_count,
      created_at :: timestamp                       as merge_request_diff_created_at,
      updated_at :: timestamp                       as merge_request_diff_updated_at,
      TO_TIMESTAMP(_updated_at :: integer)          as merge_request_diffs_last_updated_at

    FROM source


)

SELECT *
FROM renamed