{{ config({
        "materialized": "incremental",
        "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'audit_events') }}
  
  {% if is_incremental() %}

  WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1

), sequence AS (

    {{ dbt_utils.generate_series(upper_bound=11) }}

), details_parsed AS (

    SELECT
      id::INTEGER                                                                 AS audit_event_id,
      REGEXP_SUBSTR(details, '\\:([a-z_]*)\\: (.*)', 1, generated_number, 'c', 1) AS key_name,
      REGEXP_SUBSTR(details, '\\:([a-z_]*)\\: (.*)', 1, generated_number, 'c', 2) AS key_value,
      created_at::TIMESTAMP                                                       AS created_at
    FROM source
    INNER JOIN sequence
    WHERE key_name IS NOT NULL

)

SELECT *
FROM details_parsed
