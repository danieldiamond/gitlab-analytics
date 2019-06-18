WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'notification_settings') }}

), renamed AS (

    SELECT

      id :: integer                                     as notification_settings_id,
      user_id :: integer                                as user_id,
      source_id :: integer                              as source_id,
      source_type                                       as source_type,
      level :: integer                                  as settings_level,
      created_at :: timestamp                           as notification_settings_created_at,
      updated_at :: timestamp                           as notification_settings_updated_at,
      new_note :: boolean                               as has_new_note_enabled,
      new_issue :: boolean                              as has_new_issue_enabled,
      reopen_issue :: boolean                           as has_reopen_issue_enabled,
      close_issue :: boolean                            as has_close_issue_enabled,
      reassign_issue :: boolean                         as has_reassign_issue_enabled,
      new_merge_request :: boolean                      as has_new_merge_request_enabled,
      reopen_merge_request :: boolean                   as has_reopen_merge_request_enabled,
      close_merge_request :: boolean                    as has_close_merge_request_enabled,
      reassign_merge_request :: boolean                 as has_reassign_merge_request_enabled,
      merge_merge_request :: boolean                    as has_merge_merge_request_enabled,
      failed_pipeline :: boolean                        as has_failed_pipeline_enabled,
      success_pipeline :: boolean                       as has_success_pipeline_enabled


    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed