WITH source AS (

  SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'merge_requests') }}

), renamed AS (

    SELECT

      id::integer                                                AS merge_request_id,
      iid::integer                                               AS merge_request_iid,
      IFF(lower(target_branch) = 'master', TRUE, FALSE)          AS is_merge_to_master,
      IFF(lower(merge_error) = 'nan', NULL, merge_error)         AS merge_error,
      assignee_id::integer                                       AS assignee_id,
      updated_by_id::integer                                     AS updated_by_id,
      merge_user_id::integer                                     AS merge_user_id,
      last_edited_by_id::integer                                 AS last_edited_by_id,
      milestone_id::integer                                      AS milestone_id,
      head_pipeline_id::integer                                  AS head_pipeline_id,
      latest_merge_request_diff_id::integer                      AS latest_merge_request_diff_id,
      approvals_before_merge::integer                            AS approvals_before_merge,
      lock_version::integer                                      AS lock_version,
      time_estimate::integer                                     AS time_estimate,
      source_project_id::integer                                 AS project_id,
      target_project_id::integer                                 AS target_project_id,
      author_id::integer                                         AS author_id,
      state                                                      AS merge_request_state,
      merge_status                                               AS merge_request_status,
      merge_when_pipeline_succeeds::boolean                      AS does_merge_when_pipeline_succeeds,
      squash::boolean                                            AS does_squash,
      discussion_locked::boolean                                 AS is_discussion_locked,
      allow_maintainer_to_push::boolean                          AS does_allow_maintainer_to_push,
      created_at::timestamp                                      AS merge_request_created_at,
      updated_at::timestamp                                      AS merge_request_updated_at,
      last_edited_at::timestamp                                  AS merge_request_last_edited_at

      --merge_params // hidden for privacy

    FROM source
    WHERE rank_in_key = 1

)

SELECT  *
FROM renamed
