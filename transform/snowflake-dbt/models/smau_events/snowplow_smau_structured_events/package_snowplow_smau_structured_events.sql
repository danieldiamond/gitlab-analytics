{{ config({
    "materialized": "incremental",
    "unique_key": "event_id"
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
    AND derived_tstamp >= (SELECT MAX(derived_tstamp) FROM {{this}})
    AND 
      (
        (
          event_action IN 
            (
              'registry_repository_delete',
              'bulk_registry_tag_delete',
              'registry_tag_delete',
              'list_repositories',
              'delete_repository'
            )
        )
      OR
        (
          event_label IN
            (
              'bulk_registry_tag_delete',
              'registry_repository_delete',
              'registry_tag_delete'
              
            )
        )
      )
  {% endif %}

)

SELECT *
FROM snowplow_structured_events
