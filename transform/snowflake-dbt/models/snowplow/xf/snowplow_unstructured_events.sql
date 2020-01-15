{{config({
    "schema": current_date_schema('snowplow')
  })
}}

WITH events AS (

    SELECT *
    FROM {{ref('snowplow_unnested_events')}}

)

, renamed AS (

    SELECT

      event_id,
      event_name,
      TRY_PARSE_JSON(unstruct_event) AS unstruct_event,
      TRY_PARSE_JSON(unstruct_event)['data']['data']
                                     AS unstruct_event_data,
      v_tracker,
      dvce_created_tstamp,
      derived_tstamp,
      collector_tstamp,
      user_id                        AS user_custom_id,
      domain_userid                  AS user_snowplow_domain_id,
      network_userid                 AS user_snowplow_crossdomain_id,
      domain_sessionid               AS session_id,
      domain_sessionidx              AS session_index,
      page_urlhost || page_urlpath   AS page_url,
      page_urlscheme                 AS page_url_scheme,
      page_urlhost                   AS page_url_host,
      page_urlpath                   AS page_url_path,
      page_urlfragment               AS page_url_fragment,
      mkt_medium                     AS marketing_medium,
      mkt_source                     AS marketing_source,
      mkt_term                       AS marketing_term,
      mkt_content                    AS marketing_content,
      mkt_campaign                   AS marketing_campaign,
      app_id,
      br_family                      AS browser_name,
      br_name                        AS browser_major_version,
      br_version                     AS browser_minor_version,
      os_family                      AS os,
      os_name                        AS os_name,
      br_lang                        AS browser_language,
      os_manufacturer,
      os_timezone,
      br_renderengine                AS browser_engine,
      dvce_type                      AS device_type,
      dvce_ismobile                  AS device_is_mobile

      --change_form
      cf_formid,
      cf_elementid,
      cf_nodename,
      cf_type,
      cf_elementclasses,
      cf_value,
      --submit_form
      sf_formid,
      sf_formclasses,
      sf_elements,
      --focus_form
      ff_formid,
      ff_elementid,
      ff_nodename,
      ff_elementtype,
      ff_elementclasses,
      ff_value,
      --link_click
      lc_elementcontent,
      lc_elementid,
      lc_elementclasses,
      lc_elementtarget,
      lc_targeturl

    FROM events
    WHERE event = 'unstruct'

)

SELECT *
FROM renamed
