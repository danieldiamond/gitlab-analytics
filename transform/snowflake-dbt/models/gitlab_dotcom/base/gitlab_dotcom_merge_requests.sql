{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'merge_requests') }}

), renamed AS (

    SELECT

      id::INTEGER                                                AS merge_request_id,
      iid::INTEGER                                               AS merge_request_iid,
      title::VARCHAR                                             AS merge_request_title,

      IFF(lower(target_branch) = 'master', TRUE, FALSE)          AS is_merge_to_master,
      IFF(lower(merge_error) = 'nan', NULL, merge_error)         AS merge_error,
      assignee_id::INTEGER                                       AS assignee_id,
      updated_by_id::INTEGER                                     AS updated_by_id,
      merge_user_id::INTEGER                                     AS merge_user_id,
      last_edited_by_id::INTEGER                                 AS last_edited_by_id,
      milestone_id::INTEGER                                      AS milestone_id,
      head_pipeline_id::INTEGER                                  AS head_pipeline_id,
      latest_merge_request_diff_id::INTEGER                      AS latest_merge_request_diff_id,
      approvals_before_merge::INTEGER                            AS approvals_before_merge,
      lock_version::INTEGER                                      AS lock_version,
      time_estimate::INTEGER                                     AS time_estimate,
      source_project_id::INTEGER                                 AS project_id,
      target_project_id::INTEGER                                 AS target_project_id,
      author_id::INTEGER                                         AS author_id,
      state                                                      AS merge_request_state,
      merge_status                                               AS merge_request_status,
      merge_when_pipeline_succeeds::BOOLEAN                      AS does_merge_when_pipeline_succeeds,
      squash::BOOLEAN                                            AS does_squash,
      discussion_locked::BOOLEAN                                 AS is_discussion_locked,
      allow_maintainer_to_push::BOOLEAN                          AS does_allow_maintainer_to_push,
      created_at::TIMESTAMP                                      AS merge_request_created_at,
      updated_at::TIMESTAMP                                      AS merge_request_updated_at,
      last_edited_at::TIMESTAMP                                  AS merge_request_last_edited_at

      --merge_params // hidden for privacy

    FROM source
    WHERE rank_in_key = 1

)

SELECT  *
FROM renamed
