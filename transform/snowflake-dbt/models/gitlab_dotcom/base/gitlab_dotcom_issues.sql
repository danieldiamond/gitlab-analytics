{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY LAST_EDITED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'issues') }}
    WHERE created_at::varchar NOT IN ('0001-01-01 12:00:00','1000-01-01 12:00:00','10000-01-01 12:00:00')
    AND LEFT(created_at::varchar , 10) != '1970-01-01'

), renamed AS (

    SELECT

      id::INTEGER                                               AS issue_id,
      iid::INTEGER                                              AS issue_iid,
      author_id::INTEGER                                        AS author_id,
      source.project_id::INTEGER                                AS project_id,
      milestone_id::INTEGER                                     AS milestone_id,
      updated_by_id::INTEGER                                    AS updated_by_id,
      last_edited_by_id::INTEGER                                AS last_edited_by_id,
      moved_to_id::INTEGER                                      AS moved_to_id,
      created_at::timestamp                                     AS issue_created_at,
      updated_at::timestamp                                     AS issue_updated_at,
      last_edited_at::timestamp                                 AS last_edited_at,
      closed_at::timestamp                                      AS issue_closed_at,
      confidential::boolean                                     AS is_confidential,
      title,
      description,
      state,
      weight::number                                            AS weight,
      due_date::date                                            AS due_date,
      lock_version::number                                      AS lock_version,
      time_estimate::number                                     AS time_estimate,
      discussion_locked::boolean                                AS has_discussion_locked

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
