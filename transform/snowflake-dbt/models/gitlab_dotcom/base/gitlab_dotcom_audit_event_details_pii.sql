{{ config({
    "schema": "sensitive"
    })
}}

WITH base AS (

    SELECT * 
    FROM {{ ref('gitlab_dotcom_audit_event_details') }}

), audit_event_pii AS (

    SELECT
      audit_event_id,
      key_name,
      {{ nohash_sensitive_columns('gitlab_dotcom_audit_event_details', 'key_value') }},
      updated_at
    FROM base
    WHERE key_name = 'target_details'

)

SELECT *
FROM audit_event_pii