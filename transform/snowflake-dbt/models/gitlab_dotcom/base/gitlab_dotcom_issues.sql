WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY LAST_EDITED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'issues') }}
	WHERE created_at::varchar NOT IN ('0001-01-01 12:00:00','1000-01-01 12:00:00','10000-01-01 12:00:00')
	AND LEFT(created_at::varchar , 10) != '1970-01-01'

), renamed AS (

    SELECT

      id :: integer                                               as issue_id,
      author_id :: integer                                        as author_id,
      source.project_id :: integer                                as project_id,
      milestone_id :: integer                                     as milestone_id,
      updated_by_id :: integer                                    as updated_by_id,
      last_edited_by_id :: integer                                as last_edited_by_id,
      moved_to_id :: integer                                      as moved_to_id,
      created_at :: timestamp                                     as issue_created_at,
      updated_at :: timestamp                                     as issue_updated_at,
      last_edited_at :: timestamp                                 as last_edited_at,
      closed_at :: timestamp                                      as issue_closed_at,
      confidential :: boolean                                     as is_confidential,
      title,
      description,
      state,
      weight :: number                                            as weight,
      due_date :: date                                            as due_date,
      lock_version :: number                                      as lock_version,
      time_estimate :: number                                     as time_estimate,
      discussion_locked :: boolean                                as has_discussion_locked

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
