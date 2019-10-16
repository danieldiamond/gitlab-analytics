{{ config({
    "materialized": "incremental",
    "unique_key": "user_id"
    })
}}

WITH source AS (

    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
    FROM {{ source('gitlab_dotcom', 'events') }}
    
      {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(event_updated_at) FROM {{this}})

      {% endif %}

), renamed AS (

    SELECT
      id                      AS event_id,
      project_id::INTEGER     AS project_id,
      author_id::INTEGER      AS author_id,
      target_id::INTEGER      AS target_id,
      target_type             AS target_type,
      created_at::TIMESTAMP   AS event_created_at,
      updated_at::TIMESTAMP   AS event_updated_at,
      action::INTEGER         AS event_action_type_id,
      {{action_type(action_type_id='event_action_type_id')}} AS event_action_type
      
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
