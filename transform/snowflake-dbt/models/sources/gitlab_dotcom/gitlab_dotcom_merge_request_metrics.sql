WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'merge_request_metrics') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1


), renamed AS (

    SELECT

      id::INTEGER                                              AS merge_request_metric_id,
      merge_request_id::INTEGER                                AS merge_request_id,

      latest_build_started_at::TIMESTAMP                       AS latest_build_started_at,
      latest_build_finished_at::TIMESTAMP                      AS latest_build_finished_at,
      first_deployed_to_production_at::TIMESTAMP               AS first_deployed_to_production_at,
      merged_at::TIMESTAMP                                     AS merged_at,
      created_at::TIMESTAMP                                    AS created_at,
      updated_at::TIMESTAMP                                    AS updated_at,
      latest_closed_at::TIMESTAMP                              AS latest_closed_at,
      first_comment_at::TIMESTAMP                              AS first_comment_at,
      first_commit_at::TIMESTAMP                               AS first_commit_at,
      last_commit_at::TIMESTAMP                                AS last_commit_at,
      first_approved_at::TIMESTAMP                             AS first_approved_at,
      first_reassigned_at::TIMESTAMP                           AS first_reassigned_at,

      pipeline_id::INTEGER                                     AS pipeline_id,
      merged_by_id::INTEGER                                    AS merged_by_id,
      latest_closed_by_id::INTEGER                             AS latest_closed_by_id,
      diff_size::INTEGER                                       AS diff_size,
      modified_paths_size::INTEGER                             AS modified_paths_size,
      commits_count::INTEGER                                   AS commits_count,
      added_lines::INTEGER                                     AS added_lines,
      removed_lines::INTEGER                                   AS removed_lines

    FROM source

)

SELECT *
FROM renamed
