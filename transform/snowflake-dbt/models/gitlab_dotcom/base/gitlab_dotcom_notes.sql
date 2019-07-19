{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'notes') }}

), renamed AS (

    SELECT
      id::integer                                           AS note_id,
      note::varchar                                         AS note,
      IFF(noteable_type = '', NULL, noteable_type)::varchar AS noteable_type,
      author_id::integer                                    AS note_author_id,
      created_at::timestamp                                 AS note_created_at,
      updated_at::timestamp                                 AS note_updated_at,
      project_id::integer                                   AS note_project_id,
      attachment::varchar                                   AS attachment,
      line_code::varchar                                    AS line_code,
      commit_id::varchar                                    AS commit_id,
      noteable_id::integer                                  AS noteable_id,
      system::boolean                                       AS system,
      --st_diff (hidden because not relevant to our current analytics needs)
      updated_by_id::integer                                AS note_updated_by_id,
      --type (hidden because legacy and can be easily confused with noteable_type)
      position::varchar                                     AS position,
      original_position::varchar                            AS original_position,
      resolved_at::timestamp                                AS resolved_at,
      resolved_by_id::integer                               AS resolved_by_id,
      discussion_id::varchar                                AS discussion_id,
      note_html::varchar                                    AS note_html,
      cached_markdown_version::integer                      AS cached_markdown_version,
      resolved_by_push::boolean                             AS resolved_by_push
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
ORDER BY note_created_at