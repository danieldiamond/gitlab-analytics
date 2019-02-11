WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.notification_settings

), renamed AS (

    SELECT

      id :: integer                                     as notification_settings_id,
      user_id :: integer                                as user_id,
      TRY_CAST(source_id as integer)                    as source_id,
      TRY_CAST(source_type as integer)                  as source_type,
      level :: integer                                  as settings_level,
      created_at :: timestamp                           as notification_settings_created_at,
      updated_at :: timestamp                           as notification_settings_updated_at,

      TRY_CAST(new_note as boolean)                     as has_new_note_enabled,
      TRY_CAST(new_issue as boolean)                    as has_new_issue_enabled,
      TRY_CAST(reopen_issue as boolean)                 as has_reopen_issue_enabled,
      TRY_CAST(close_issue as boolean)                  as has_close_issue_enabled,
      TRY_CAST(reassign_issue as boolean)               as has_reassign_issue_enabled,
      TRY_CAST(new_merge_request as boolean)            as has_new_merge_request_enabled,
      TRY_CAST(reopen_merge_request as boolean)         as has_reopen_merge_request_enabled,
      TRY_CAST(close_merge_request as boolean)          as has_close_merge_request_enabled,
      TRY_CAST(reassign_merge_request as boolean)       as has_reassign_merge_request_enabled,
      TRY_CAST(merge_merge_request as boolean)          as has_merge_merge_request_enabled,
      TRY_CAST(failed_pipeline as boolean)              as has_failed_pipeline_enabled,
      TRY_CAST(success_pipeline as boolean)             as has_success_pipeline_enabled


    FROM source


)

SELECT *
FROM renamed