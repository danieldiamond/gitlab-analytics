{{ config({
    "materialized": "incremental",
    "unique_key": "audit_event_id",
    "schema": "analytics"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'audit_events') }}
  
  {% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

  SELECT
    id::INTEGER             AS audit_event_id,
    author_id::INTEGER      AS author_id,
    type::VARCHAR           AS audit_event_type,
    entity_id::INTEGER      AS entity_id,
    entity_type::VARCHAR    AS entity_type,
    created_at::TIMESTAMP   AS created_at,
    updated_at::TIMESTAMP   AS updated_at

  FROM source

)

SELECT *
FROM renamed
ORDER BY updated_at
