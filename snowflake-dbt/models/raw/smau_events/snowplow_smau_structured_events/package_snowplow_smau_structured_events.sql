{{ config({
    "materialized": "incremental",
    "unique_key": "event_surrogate_key"
    })
}}

WITH snowplow_structured_events AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    derived_tstamp,
    page_url_path,
    event_id,
    event_action,
    event_label
  FROM {{ ref('snowplow_structured_events')}}
  WHERE derived_tstamp >= '2019-01-01'
  {% if is_incremental() %}
    AND derived_tstamp >= (SELECT MAX({{this}}.event_date) FROM {{this}})
  {% endif %}
    AND 
      (
        event_action IN 
          (
            'delete_repository',
            'delete_tag',
            'delete_tag_bulk',
            'list_repositories',
            'list_tags'
          )
        
        OR
        
        event_label IN
          (
            'bulk_registry_tag_delete',
            'registry_repository_delete',
            'registry_tag_delete'
            
          )
        
      )

)

, renamed AS (
  
    SELECT
      user_snowplow_domain_id,
      user_custom_id,
      TO_DATE(derived_tstamp) AS event_date,
      page_url_path,
      event_action 
        || IFF(event_label IS NOT NULL, 
                '_' || event_label
                , NULL)       AS event_type,
      event_id                AS event_surrogate_key
    FROM snowplow_structured_events
    
)

SELECT *
FROM renamed
