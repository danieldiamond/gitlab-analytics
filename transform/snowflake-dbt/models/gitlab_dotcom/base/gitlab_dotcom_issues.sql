{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'issues') }}
  WHERE created_at::VARCHAR NOT IN ('0001-01-01 12:00:00','1000-01-01 12:00:00','10000-01-01 12:00:00')
    AND LEFT(created_at::VARCHAR , 10) != '1970-01-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER                                               AS issue_id,
      iid::INTEGER                                              AS issue_iid,
      author_id::INTEGER                                        AS issue_author_id,
      source.project_id::INTEGER                                AS project_id,
      milestone_id::INTEGER                                     AS milestone_id,
      updated_by_id::INTEGER                                    AS updated_by_id,
      last_edited_by_id::INTEGER                                AS last_edited_by_id,
      moved_to_id::INTEGER                                      AS moved_to_id,
      created_at::TIMESTAMP                                     AS created_at,
      updated_at::TIMESTAMP                                     AS updated_at,
      last_edited_at::TIMESTAMP                                 AS issue_last_edited_at,
      closed_at::TIMESTAMP                                      AS issue_closed_at,
      confidential::BOOLEAN                                     AS is_confidential,
      title::VARCHAR                                            AS issue_title,
      description::VARCHAR                                      AS issue_description,
      state::VARCHAR                                            AS issue_state,
      weight::NUMBER                                            AS weight,
      due_date::DATE                                            AS due_date,
      lock_version::NUMBER                                      AS lock_version,
      time_estimate::NUMBER                                     AS time_estimate,
      discussion_locked::BOOLEAN                                AS has_discussion_locked

    FROM source

)

SELECT *
FROM renamed
