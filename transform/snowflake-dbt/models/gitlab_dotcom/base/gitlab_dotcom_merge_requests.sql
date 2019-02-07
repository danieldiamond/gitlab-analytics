WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.merge_requests

), renamed AS (

    SELECT

      id :: integer                                                as merge_request_id,



      IFF(lower(target_branch) = 'master', TRUE, FALSE)            as is_merge_to_master,
      IFF(lower(merge_error) = 'nan', NULL, merge_error)           as merge_error,


      TRY_CAST(assignee_id as integer)                             as assignee_id,
      TRY_CAST(updated_by_id as integer)                           as updated_by_id,
      TRY_CAST(merge_user_id as integer)                           as merge_user_id,
      TRY_CAST(last_edited_by_id as integer)                       as last_edited_by_id,
      TRY_CAST(milestone_id as integer)                            as milestone_id,
      TRY_CAST(head_pipeline_id as integer)                        as head_pipeline_id,
      TRY_CAST(latest_merge_request_diff_id as integer)            as latest_merge_request_diff_id,
      TRY_CAST(approvals_before_merge as integer)                  as approvals_before_merge,
      TRY_CAST(lock_version as integer)                            as lock_version,
      TRY_CAST(time_estimate as integer)                           as time_estimate,
      TRY_CAST(source_project_id as integer)                       as project_id,
      TRY_CAST(target_project_id as integer)                       as target_project_id,
      TRY_CAST(author_id as integer)                               as author_id,

      state                                                        as merge_request_state,
      merge_status                                                 as merge_request_status,

      TRY_CAST(merge_when_pipeline_succeeds as boolean)            as does_merge_when_pipeline_succeeds,
      TRY_CAST(squash as boolean)                                  as does_squash,
      TRY_CAST(discussion_locked as boolean)                       as is_discussion_locked,
      TRY_CAST(allow_maintainer_to_push as boolean)                as does_allow_maintainer_to_push,

      TRY_CAST(created_at as timestamp)                            as merge_request_created_at,
      TRY_CAST(updated_at as timestamp)                            as merge_request_updated_at,
      TRY_CAST(last_edited_at as timestamp)                        as merge_request_last_edited_at,

      --merge_params // hidden for privacy

      TO_TIMESTAMP(_updated_at :: integer)                         as merge_requests_last_updated_at

    FROM source


)

SELECT *
FROM renamed