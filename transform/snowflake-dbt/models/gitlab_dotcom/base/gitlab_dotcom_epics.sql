WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'epics') }}

), renamed AS (

    SELECT
      id :: integer                                   as epic_id,
      milestone_id :: integer                         as milestone_id,
      group_id :: integer                             as group_id,
      author_id :: integer                            as author_id,
      assignee_id :: integer                          as assignee_id,
      iid :: integer                                  as epic_internal_id,
      updated_by_id :: integer                        as updated_by_id,
      last_edited_by_id :: integer                    as last_edited_by_id,
      lock_version :: integer                         as lock_version,
      start_date :: date                              as epic_start_date,
      end_date :: date                                as epic_end_date,
      last_edited_at :: timestamp                     as epic_last_edited_at,
      created_at :: timestamp                         as epic_created_at,
      updated_at :: timestamp                         as epic_updated_at,
      LENGTH(title)                                   as epic_title_length,
      LENGTH(description)                             as epic_description_length

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed