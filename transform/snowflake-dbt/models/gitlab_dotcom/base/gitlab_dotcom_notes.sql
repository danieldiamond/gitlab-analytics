{{ config({
    "materialized": "incremental",
    "unique_key": "note_id"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'notes') }}

  {% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(note_updated_at) FROM {{this}})

  {% endif %}

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
      cached_markdown_version::integer                      AS cached_markdown_version,
      resolved_by_push::boolean                             AS resolved_by_push
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
WHERE note_id NOT IN (
                      203215238 --https://gitlab.com/gitlab-data/analytics/merge_requests/1423
                     )
ORDER BY note_created_at
