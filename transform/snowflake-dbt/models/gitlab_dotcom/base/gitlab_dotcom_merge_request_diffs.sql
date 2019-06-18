WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'merge_request_diffs') }}
  WHERE created_at IS NOT NULL
    AND updated_at IS NOT NULL


), renamed AS (

    SELECT

      id :: integer                                 as merge_request_diff_id,
      base_commit_sha,
      head_commit_sha,
      start_commit_sha,
      state                                         as merge_request_diff_status,
      merge_request_id :: integer                   as merge_request_id,
      real_size                                     as merge_request_real_size,
      commits_count :: integer                      as commits_count,
      created_at :: timestamp                       as merge_request_diff_created_at,
      updated_at :: timestamp                       as merge_request_diff_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed