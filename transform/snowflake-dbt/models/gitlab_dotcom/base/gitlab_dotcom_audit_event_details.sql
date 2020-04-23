{{ config({
        "materialized": "incremental",
        "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'audit_events') }}
  
  {% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), numbers AS (

    SELECT ROW_NUMBER() OVER (ORDER BY id DESC) rn
    FROM {{ source('gitlab_dotcom', 'audit_events') }}
    ORDER BY rn ASC
    LIMIT 11


), details_parsed AS (

    SELECT
      id::INTEGER                                                   AS audit_event_id,
      REGEXP_SUBSTR(details, '\\:([a-z_]*)\\: (.*)', 1, rn, 'c', 1) AS key_name,
      REGEXP_SUBSTR(details, '\\:([a-z_]*)\\: (.*)', 1, rn, 'c', 2) AS key_value
    FROM source
    INNER JOIN numbers
    WHERE key_name IS NOT NULL

)

SELECT *
FROM details_parsed
