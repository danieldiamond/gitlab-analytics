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
      id::NUMBER                                   AS epic_id,
      group_id::NUMBER                             AS group_id,
      author_id::NUMBER                            AS author_id,
      assignee_id::NUMBER                          AS assignee_id,
      iid::NUMBER                                  AS epic_internal_id,
      updated_by_id::NUMBER                        AS updated_by_id,
      last_edited_by_id::NUMBER                    AS last_edited_by_id,
      lock_version::NUMBER                         AS lock_version,
      start_date::DATE                              AS epic_start_date,
      end_date::DATE                                AS epic_end_date,
      last_edited_at::TIMESTAMP                     AS epic_last_edited_at,
      created_at::TIMESTAMP                         AS created_at,
      updated_at::TIMESTAMP                         AS updated_at,
      title::VARCHAR                                AS epic_title,
      description::VARCHAR                          AS epic_description,
      closed_at::TIMESTAMP                          AS closed_at,
      state_id::NUMBER                             AS state_id,
      parent_id::NUMBER                            AS parent_id,
      relative_position::NUMBER                    AS relative_position,
      start_date_sourcing_epic_id::NUMBER          AS start_date_sourcing_epic_id,
      external_key::VARCHAR                         AS external_key,
      confidential::BOOLEAN                         AS is_confidential,
      {{ map_state_id('state_id') }}                AS state,
      LENGTH(title)::NUMBER                        AS epic_title_length,
      LENGTH(description)::NUMBER                  AS epic_description_length

    FROM source

)

SELECT *
FROM renamed
