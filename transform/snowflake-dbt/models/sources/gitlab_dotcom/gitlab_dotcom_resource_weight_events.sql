{{ config({
    "materialized": "incremental",
    "unique_key": "resource_weight_event_id"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'resource_weight_events') }}
    {% if is_incremental() %}
      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})
    {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

)

, renamed AS (

    SELECT
      id                                             AS resource_weight_event_id,
      user_id::NUMBER                               AS user_id,
      issue_id::NUMBER                              AS issue_id,
      weight::NUMBER                                AS weight,
      created_at::TIMESTAMP                          AS created_at
    FROM source

)

SELECT *
FROM renamed
