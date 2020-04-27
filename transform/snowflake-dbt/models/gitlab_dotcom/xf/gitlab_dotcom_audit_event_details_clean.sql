WITH non_pii_details AS (

    SELECT
      audit_event_id,
      key_name,
      key_value,
      updated_at
    FROM {{ ref('gitlab_dotcom', 'audit_event_details') }}
    WHERE key_name != 'target_details'

), pii_details AS (

    SELECT 
      audit_event_id,
      key_name,
      key_value_hash AS key_value,
      updated_at
    FROM {{ ref('gitlab_dotcom', 'audit_event_details_pii') }}

), unioned AS (

    SELECT *
    FROM non_pii_details
    UNION ALL
    SELECT *
    FROM pii_details

)

SELECT *
FROM unioned