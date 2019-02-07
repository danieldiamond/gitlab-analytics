WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.epics

), renamed AS (

    SELECT
      id :: integer                                   as epic_id,
      TRY_CAST(milestone_id as integer)               as milestone_id,
      TRY_CAST(group_id as integer)                   as group_id,
      TRY_CAST(author_id as integer)                  as author_id,
      TRY_CAST(assignee_id as integer)                as assignee_id,
      TRY_CAST(iid as integer)                        as epic_internal_id,
      TRY_CAST(updated_by_id as integer)              as updated_by_id,
      TRY_CAST(last_edited_by_id as integer)          as last_edited_by_id,
      TRY_CAST(lock_version as integer)               as lock_version,
      TRY_CAST(start_date as date)                    as epic_start_date,
      TRY_CAST(end_date as date)                      as epic_end_date,
      TRY_CAST(last_edited_at as timestamp)           as epic_last_edited_at,
      created_at :: timestamp                         as epic_created_at,
      updated_at :: timestamp                         as epic_updated_at,
      LENGTH(title)                                   as epic_title_length,
      LENGTH(description)                             as epic_description_length,
      TO_TIMESTAMP(_updated_at :: int)                as epics_last_updated

    FROM source


)

SELECT *
FROM renamed