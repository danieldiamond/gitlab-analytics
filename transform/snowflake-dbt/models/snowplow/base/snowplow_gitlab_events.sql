{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "unique_key":"event_id",
    "schema":current_date_schema('snowplow'),
    "materialized": "table"
  })
}}

{% set change_form = ['formId','elementId','nodeName','type','elementClasses','value'] %}
{% set submit_form = ['formId','formClasses','elements'] %}
{% set focus_form = ['formId','elementId','nodeName','elementType','elementClasses','value'] %}
{% set link_click = ['elementId','elementClasses','elementTarget','targetUrl','elementContent'] %}
{% set track_timing = ['category','variable','timing','label'] %}


WITH source as (

    SELECT DISTINCT
      app_id,
      base_currency,
      br_colordepth,
      br_cookies,
      br_family,
      br_features_director,
      br_features_flash,
      br_features_gears,
      br_features_java,
      br_features_pdf,
      br_features_quicktime,
      br_features_realplayer,
      br_features_silverlight,
      br_features_windowsmedia,
      br_lang,
      br_name,
      br_renderengine,
      br_type,
      br_version,
      br_viewheight,
      br_viewwidth,
      collector_tstamp,
      contexts,
      derived_contexts,
      -- correctting bugs on ruby tracker which was sending wrong timestamp
      -- https://gitlab.com/gitlab-data/analytics/issues/3097
      IFF(DATE_PART('year', TRY_TO_TIMESTAMP(derived_tstamp)) > 1970, 
            derived_tstamp, collector_tstamp) AS derived_tstamp,
      doc_charset,
      try_to_numeric(doc_height)              AS doc_height,
      try_to_numeric(doc_width)               AS doc_width,
      domain_sessionid,
      domain_sessionidx,
      domain_userid,
      dvce_created_tstamp,
      dvce_ismobile,
      dvce_screenheight,
      dvce_screenwidth,
      dvce_sent_tstamp,
      dvce_type,
      etl_tags,
      etl_tstamp,
      event,
      event_fingerprint,
      event_format,
      event_id,
      try_parse_json(contexts)['data'][0]['data']['id']::varchar AS web_page_id,
      event_name,
      event_vendor,
      event_version,
      geo_city,
      geo_country,
      geo_latitude,
      geo_longitude,
      geo_region,
      geo_region_name,
      geo_timezone,
      geo_zipcode,
      ip_domain,
      ip_isp,
      ip_netspeed,
      ip_organization,
      mkt_campaign,
      mkt_clickid,
      mkt_content,
      mkt_medium,
      mkt_network,
      mkt_source,
      mkt_term,
      name_tracker,
      network_userid,
      os_family,
      os_manufacturer,
      os_name,
      os_timezone,
      page_referrer,
      page_title,
      page_url,
      page_urlfragment,
      page_urlhost,
      page_urlpath,
      page_urlport,
      page_urlquery,
      page_urlscheme,
      platform,
      try_to_numeric(pp_xoffset_max)          AS pp_xoffset_max,
      try_to_numeric(pp_xoffset_min)          AS pp_xoffset_min,
      try_to_numeric(pp_yoffset_max)          AS pp_yoffset_max,
      try_to_numeric(pp_yoffset_min)          AS pp_yoffset_min,
      refr_domain_userid,
      refr_dvce_tstamp,
      refr_medium,
      refr_source,
      refr_term,
      refr_urlfragment,
      refr_urlhost,
      refr_urlpath,
      refr_urlport,
      refr_urlquery,
      refr_urlscheme,
      se_action,
      se_category,
      se_label,
      se_property,
      se_value,
      ti_category,
      ti_currency,
      ti_name,
      ti_orderid,
      ti_price,
      ti_price_base,
      ti_quantity,
      ti_sku,
      tr_affiliation,
      tr_city,
      tr_country,
      tr_currency,
      tr_orderid,
      tr_shipping,
      tr_shipping_base,
      tr_state,
      tr_tax,
      tr_tax_base,
      tr_total,
      tr_total_base,
      true_tstamp,
      txn_id,
      CASE
        WHEN event_name IN ('submit_form', 'focus_form', 'change_form')
          THEN 'masked'
        ELSE unstruct_event
      END AS unstruct_event,
      user_fingerprint,
      user_id,
      user_ipaddress,
      useragent,
      v_collector,
      v_etl,
      v_tracker,
      uploaded_at,
      'GitLab' AS infra_source
    {% if target.name not in ("prod") -%}

    FROM {{ source('gitlab_snowplow', 'events_sample') }}

    {%- else %}

    FROM {{ source('gitlab_snowplow', 'events') }}

    {%- endif %}
)

, base AS (
  
    SELECT * 
    FROM source
    WHERE app_id IS NOT NULL
      AND date_part(month, try_to_timestamp(derived_tstamp)) = '{{ month_value }}'
      AND date_part(year, try_to_timestamp(derived_tstamp)) = '{{ year_value }}'
      AND 
        (
          (
            -- js backend tracker
            v_tracker LIKE 'js%'
            AND lower(page_url) NOT LIKE 'https://staging.gitlab.com/%'
            AND lower(page_url) NOT LIKE 'https://customers.stg.gitlab.com/%'
            AND lower(page_url) NOT LIKE 'http://localhost:%'
          )
          
          OR
          
          (
            -- ruby backend tracker
            v_tracker LIKE 'rb%'
          )
        )
      AND try_to_timestamp(derived_tstamp) is not null

), events_to_ignore as (

    SELECT event_id
    FROM base
    GROUP BY 1
    HAVING count (*) > 1

), unnested_unstruct as (

    SELECT *,
    CASE
      WHEN length(unstruct_event) > 0 AND try_parse_json(unstruct_event) IS NULL
        THEN TRUE
      ELSE FALSE END AS is_bad_unstruct_event,
    {{ unpack_unstructured_event(change_form, 'change_form', 'cf') }},
    {{ unpack_unstructured_event(submit_form, 'submit_form', 'sf') }},
    {{ unpack_unstructured_event(focus_form, 'focus_form', 'ff') }},
    {{ unpack_unstructured_event(link_click, 'link_click', 'lc') }},
    {{ unpack_unstructured_event(track_timing, 'track_timing', 'tt') }}
    FROM base

)


SELECT *
FROM unnested_unstruct
WHERE event_id NOT IN (SELECT * FROM events_to_ignore)
ORDER BY derived_tstamp
