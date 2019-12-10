{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'epics') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                                   AS epic_id,
      milestone_id::INTEGER                         AS milestone_id,
      group_id::INTEGER                             AS group_id,
      author_id::INTEGER                            AS author_id,
      assignee_id::INTEGER                          AS assignee_id,
      iid::INTEGER                                  AS epic_internal_id,
      updated_by_id::INTEGER                        AS updated_by_id,
      last_edited_by_id::INTEGER                    AS last_edited_by_id,
      lock_version::INTEGER                         AS lock_version,
      start_date::DATE                              AS epic_start_date,
      end_date::DATE                                AS epic_end_date,
      last_edited_at::TIMESTAMP                     AS epic_last_edited_at,
      created_at::TIMESTAMP                         AS created_at,
      updated_at::TIMESTAMP                         AS updated_at,
      title::VARCHAR                                AS epic_title,
      LENGTH(title)::INTEGER                        AS epic_title_length,
      LENGTH(description)::INTEGER                  AS epic_description_length

    FROM source

)

SELECT *
FROM renamed
