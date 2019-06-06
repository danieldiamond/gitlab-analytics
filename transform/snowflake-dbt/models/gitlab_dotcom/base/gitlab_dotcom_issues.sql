WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'issues') }}

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
      closed_at :: timestamp                                      as closed_at,
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