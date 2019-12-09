{{config({
    "schema": current_date_schema('snowplow')
  })
}}

WITH events AS (

    SELECT *
    FROM {{ref('snowplow_unnested_events')}}

)

SELECT
  event_id,
  v_tracker,
  se_action AS event_action,
  se_category AS event_category,
  se_label AS event_label,
  se_property AS event_prorperty,
  se_value AS event_value,
  dvce_created_tstamp,
  derived_tstamp,
  collector_tstamp,
  user_id as user_custom_id,
  domain_userid as user_snowplow_domain_id,
  network_userid as user_snowplow_crossdomain_id,
  domain_sessionid as session_id,
  domain_sessionidx as session_index,
  page_urlhost || page_urlpath as page_url,
  page_urlscheme as page_url_scheme,
  page_urlhost as page_url_host,
  page_urlpath as page_url_path,
  page_urlfragment as page_url_fragment,
  mkt_medium as marketing_medium,
  mkt_source as marketing_source,
  mkt_term as marketing_term,
  mkt_content as marketing_content,
  mkt_campaign as marketing_campaign,
  app_id,
  br_family as browser_name,
  br_name as browser_major_version,
  br_version as browser_minor_version,
  os_family as os,
  os_name as os_name,
  br_lang as browser_language,
  os_manufacturer,
  os_timezone,
  br_renderengine as browser_engine,
  dvce_type as device_type,
  dvce_ismobile as device_is_mobile
FROM events
WHERE event = 'struct'
