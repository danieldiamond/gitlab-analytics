{{ config({
    "materialized": "incremental",
    "unique_key": "merge_request_diff_id",
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'merge_request_diffs') }}
  WHERE created_at IS NOT NULL
    AND updated_at IS NOT NULL
    
    {% if is_incremental() %}

    AND updated_at >= (SELECT MAX(merge_request_diff_updated_at) FROM {{this}})

    {% endif %}

), renamed AS (

    SELECT
      id::INTEGER                                 AS merge_request_diff_id,
      base_commit_sha,
      head_commit_sha,
      start_commit_sha,
      state                                       AS merge_request_diff_status,
      merge_request_id::INTEGER                   AS merge_request_id,
      real_size                                   AS merge_request_real_size,
      commits_count::INTEGER                      AS commits_count,
      created_at::TIMESTAMP                       AS merge_request_diff_created_at,
      updated_at::TIMESTAMP                       AS merge_request_diff_updated_at
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
ORDER BY merge_request_diff_updated_at
