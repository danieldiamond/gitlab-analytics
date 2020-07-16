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

  WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1

), renamed AS (

  SELECT
    id::INTEGER             AS audit_event_id,
    author_id::INTEGER      AS author_id,
    type::VARCHAR           AS audit_event_type,
    entity_id::INTEGER      AS entity_id,
    entity_type::VARCHAR    AS entity_type,
    created_at::TIMESTAMP   AS created_at

  FROM source

)

SELECT *
FROM renamed
ORDER BY updated_at
