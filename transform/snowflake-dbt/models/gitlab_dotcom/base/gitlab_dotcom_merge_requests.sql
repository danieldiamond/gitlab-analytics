WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'merge_requests') }}

), renamed AS (

    SELECT

      id :: integer                                                as merge_request_id,
      IFF(lower(target_branch) = 'master', TRUE, FALSE)            as is_merge_to_master,
      IFF(lower(merge_error) = 'nan', NULL, merge_error)           as merge_error,
      assignee_id :: integer                                       as assignee_id,
      updated_by_id :: integer                                     as updated_by_id,
      merge_user_id :: integer                                     as merge_user_id,
      last_edited_by_id :: integer                                 as last_edited_by_id,
      milestone_id :: integer                                      as milestone_id,
      head_pipeline_id :: integer                                  as head_pipeline_id,
      latest_merge_request_diff_id :: integer                      as latest_merge_request_diff_id,
      approvals_before_merge :: integer                            as approvals_before_merge,
      lock_version :: integer                                      as lock_version,
      time_estimate :: integer                                     as time_estimate,
      source_project_id :: integer                                 as project_id,
      target_project_id :: integer                                 as target_project_id,
      author_id :: integer                                         as author_id,
      state                                                        as merge_request_state,
      merge_status                                                 as merge_request_status,
      merge_when_pipeline_succeeds :: boolean                      as does_merge_when_pipeline_succeeds,
      squash :: boolean                                            as does_squash,
      discussion_locked :: boolean                                 as is_discussion_locked,
      allow_maintainer_to_push :: boolean                          as does_allow_maintainer_to_push,
      created_at :: timestamp                                      as merge_request_created_at,
      updated_at :: timestamp                                      as merge_request_updated_at,
      last_edited_at :: timestamp                                  as merge_request_last_edited_at

      --merge_params // hidden for privacy

    FROM source
    WHERE rank_in_key = 1

)

SELECT  *
FROM renamed
