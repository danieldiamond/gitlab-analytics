WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'audit_events') }}

), renamed AS (

    SELECT
      id :: integer            AS audit_event_id,
      author_id :: integer     AS author_id,
      type :: varchar          AS audit_event_type,
      entity_id :: integer     AS entity_id,
      entity_type ::varchar    AS entity_type,
      details :: varchar       AS audit_event_details,
      created_at ::timestamp   AS audit_event_created_at,
      updated_at ::timestamp   AS audit_event_updated_at

    FROM source
    WHERE rank_in_key = 1
    ORDER BY audit_event_created_at

)

SELECT * FROM renamed
