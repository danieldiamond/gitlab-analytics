{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'resource_label_events') }}

)

, renamed AS (

    SELECT
      id                                             AS resource_label_event_id,
      action::INTEGER                                AS action_type_id,
      {{ resource_label_action_type('action') }}     AS action_type,
      issue_id::INTEGER                              AS issue_id,
      merge_request_id::INTEGER                      AS merge_request_id,
      epic_id::INTEGER                               AS epic_id,
      label_id::INTEGER                              AS label_id,
      user_id::INTEGER                               AS user_id,
      created_at::TIMESTAMP                          AS created_at,
      cached_markdown_version::VARCHAR               AS cached_markdown_version,
      reference::VARCHAR                             AS referrence,
      reference_html::VARCHAR                        AS reference_html  
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
