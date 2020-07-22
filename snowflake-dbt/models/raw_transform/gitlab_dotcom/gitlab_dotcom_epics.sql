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
      description::VARCHAR                          AS epic_description,
      closed_at::TIMESTAMP                          AS closed_at,
      state_id::INTEGER                             AS state_id,
      parent_id::INTEGER                            AS parent_id,
      relative_position::INTEGER                    AS relative_position,
      start_date_sourcing_epic_id::INTEGER          AS start_date_sourcing_epic_id,
      external_key::VARCHAR                         AS external_key,
      confidential::BOOLEAN                         AS is_confidential,
      {{ map_state_id('state_id') }}                AS state,
      LENGTH(title)::INTEGER                        AS epic_title_length,
      LENGTH(description)::INTEGER                  AS epic_description_length

    FROM source

)

SELECT *
FROM renamed
