{{ config({
    "materialized": "incremental",
    "unique_key": "note_id",
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'notes') }}

  {% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                                           AS note_id,
      note::VARCHAR                                         AS note,
      IFF(noteable_type = '', NULL, noteable_type)::VARCHAR AS noteable_type,
      author_id::INTEGER                                    AS note_author_id,
      created_at::TIMESTAMP                                 AS created_at,
      updated_at::TIMESTAMP                                 AS updated_at,
      project_id::INTEGER                                   AS project_id,
      attachment::VARCHAR                                   AS attachment,
      line_code::VARCHAR                                    AS line_code,
      commit_id::VARCHAR                                    AS commit_id,
      noteable_id::INTEGER                                  AS noteable_id,
      system::BOOLEAN                                       AS system,
      --st_diff (hidden because not relevant to our current analytics needs)
      updated_by_id::INTEGER                                AS note_updated_by_id,
      --type (hidden because legacy and can be easily confused with noteable_type)
      position::VARCHAR                                     AS position,
      original_position::VARCHAR                            AS original_position,
      resolved_at::TIMESTAMP                                AS resolved_at,
      resolved_by_id::INTEGER                               AS resolved_by_id,
      discussion_id::VARCHAR                                AS discussion_id,
      cached_markdown_version::INTEGER                      AS cached_markdown_version,
      resolved_by_push::BOOLEAN                             AS resolved_by_push
    FROM source

)

SELECT *
FROM renamed
WHERE note_id NOT IN (
  203215238 --https://gitlab.com/gitlab-data/analytics/merge_requests/1423
)
ORDER BY created_at
