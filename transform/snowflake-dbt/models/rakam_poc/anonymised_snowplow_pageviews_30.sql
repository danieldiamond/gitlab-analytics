WITH snowplow_page_views_30 AS (
  
  SELECT *
  FROM {{ ref('snowplow_page_views_30')}}

)

, configure_snowplow_smau_events AS (
  
  SELECT *
  FROM {{ ref('configure_snowplow_smau_pageviews_events')}}
  
)
, filtered_pageviews AS (
  
  SELECT *
  FROM snowplow_page_views_30
  INNER JOIN configure_snowplow_smau_events ON snowplow_page_views_30.page_view_id = configure_snowplow_smau_events.configure_snowplow_smau_events
  
)

SELECT *
FROM filtered_pageviews
