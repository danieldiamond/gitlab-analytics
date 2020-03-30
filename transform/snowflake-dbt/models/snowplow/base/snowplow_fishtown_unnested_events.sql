{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "unique_key":"event_id",
    "schema":current_date_schema('snowplow')
  })
}}

{% set change_form = ['formId','elementId','nodeName','type','elementClasses','value'] %}
{% set submit_form = ['formId','formClasses','elements'] %}
{% set focus_form = ['formId','elementId','nodeName','elementType','elementClasses','value'] %}
{% set link_click = ['elementId','elementClasses','elementTarget','targetUrl','elementContent'] %}
{% set track_timing = ['category','variable','timing','label'] %}


with base as (

SELECT
    DISTINCT
       nullif(jsontext['app_id']::STRING, '')                   AS app_id,
       nullif(jsontext['base_currency']::STRING, '')            AS base_currency,
       nullif(jsontext['br_colordepth']::STRING, '')            AS br_colordepth,
       nullif(jsontext['br_cookies']::STRING, '')               AS br_cookies,
       nullif(jsontext['br_family']::STRING, '')                AS br_family,
       nullif(jsontext['br_features_director']::STRING, '')     AS br_features_director,
       nullif(jsontext['br_features_flash']::STRING, '')        AS br_features_flash,
       nullif(jsontext['br_features_gears']::STRING, '')        AS br_features_gears,
       nullif(jsontext['br_features_java']::STRING, '')         AS br_features_java,
       nullif(jsontext['br_features_pdf']::STRING, '')          AS br_features_pdf,
       nullif(jsontext['br_features_quicktime']::STRING, '')    AS br_features_quicktime,
       nullif(jsontext['br_features_realplayer']::STRING, '')   AS br_features_realplayer,
       nullif(jsontext['br_features_silverlight']::STRING, '')  AS br_features_silverlight,
       nullif(jsontext['br_features_windowsmedia']::STRING, '') AS br_features_windowsmedia,
       nullif(jsontext['br_lang']::STRING, '')                  AS br_lang,
       nullif(jsontext['br_name']::STRING, '')                  AS br_name,
       nullif(jsontext['br_renderengine']::STRING, '')          AS br_renderengine,
       nullif(jsontext['br_type']::STRING, '')                  AS br_type,
       nullif(jsontext['br_version']::STRING, '')               AS br_version,
       nullif(jsontext['br_viewheight']::STRING, '')            AS br_viewheight,
       nullif(jsontext['br_viewwidth']::STRING, '')             AS br_viewwidth,
       nullif(jsontext['collector_tstamp']::STRING, '')         AS collector_tstamp,
       nullif(jsontext['contexts']::STRING, '')                 AS contexts,
       nullif(jsontext['derived_contexts']::STRING, '')         AS derived_contexts,
       nullif(jsontext['derived_tstamp']::STRING, '')           AS derived_tstamp,
       nullif(jsontext['doc_charset']::STRING, '')              AS doc_charset,
       try_to_numeric(jsontext['doc_height']::STRING)           AS doc_height,
       try_to_numeric(jsontext['doc_width']::STRING)            AS doc_width,
       nullif(jsontext['domain_sessionid']::STRING, '')         AS domain_sessionid,
       nullif(jsontext['domain_sessionidx']::STRING, '')        AS domain_sessionidx,
       nullif(jsontext['domain_userid']::STRING, '')            AS domain_userid,
       nullif(jsontext['dvce_created_tstamp']::STRING, '')      AS dvce_created_tstamp,
       nullif(jsontext['dvce_ismobile']::STRING, '')            AS dvce_ismobile,
       nullif(jsontext['dvce_screenheight']::STRING, '')        AS dvce_screenheight,
       nullif(jsontext['dvce_screenwidth']::STRING, '')         AS dvce_screenwidth,
       nullif(jsontext['dvce_sent_tstamp']::STRING, '')         AS dvce_sent_tstamp,
       nullif(jsontext['dvce_type']::STRING, '')                AS dvce_type,
       nullif(jsontext['etl_tags']::STRING, '')                 AS etl_tags,
       nullif(jsontext['etl_tstamp']::STRING, '')               AS etl_tstamp,
       nullif(jsontext['event']::STRING, '')                    AS event,
       nullif(jsontext['event_fingerprint']::STRING, '')        AS event_fingerprint,
       nullif(jsontext['event_format']::STRING, '')             AS event_format,
       nullif(jsontext['event_id']::STRING, '')                 AS event_id,
       left(right(jsontext['contexts'], 41), 36)                AS web_page_id,
       nullif(jsontext['event_name']::STRING, '')               AS event_name,
       nullif(jsontext['event_vendor']::STRING, '')             AS event_vendor,
       nullif(jsontext['event_version']::STRING, '')            AS event_version,
       nullif(jsontext['geo_city']::STRING, '')                 AS geo_city,
       nullif(jsontext['geo_country']::STRING, '')              AS geo_country,
       nullif(jsontext['geo_latitude']::STRING, '')             AS geo_latitude,
       nullif(jsontext['geo_longitude']::STRING, '')            AS geo_longitude,
       nullif(jsontext['geo_region']::STRING, '')               AS geo_region,
       nullif(jsontext['geo_region_name']::STRING, '')          AS geo_region_name,
       nullif(jsontext['geo_timezone']::STRING, '')             AS geo_timezone,
       nullif(jsontext['geo_zipcode']::STRING, '')              AS geo_zipcode,
       nullif(jsontext['ip_domain']::STRING, '')                AS ip_domain,
       nullif(jsontext['ip_isp']::STRING, '')                   AS ip_isp,
       nullif(jsontext['ip_netspeed']::STRING, '')              AS ip_netspeed,
       nullif(jsontext['ip_organization']::STRING, '')          AS ip_organization,
       nullif(jsontext['mkt_campaign']::STRING, '')             AS mkt_campaign,
       nullif(jsontext['mkt_clickid']::STRING, '')              AS mkt_clickid,
       nullif(jsontext['mkt_content']::STRING, '')              AS mkt_content,
       nullif(jsontext['mkt_medium']::STRING, '')               AS mkt_medium,
       nullif(jsontext['mkt_network']::STRING, '')              AS mkt_network,
       nullif(jsontext['mkt_source']::STRING, '')               AS mkt_source,
       nullif(jsontext['mkt_term']::STRING, '')                 AS mkt_term,
       nullif(jsontext['name_tracker']::STRING, '')             AS name_tracker,
       nullif(jsontext['network_userid']::STRING, '')           AS network_userid,
       nullif(jsontext['os_family']::STRING, '')                AS os_family,
       nullif(jsontext['os_manufacturer']::STRING, '')          AS os_manufacturer,
       nullif(jsontext['os_name']::STRING, '')                  AS os_name,
       nullif(jsontext['os_timezone']::STRING, '')              AS os_timezone,
       nullif(jsontext['page_referrer']::STRING, '')            AS page_referrer,
       nullif(jsontext['page_title']::STRING, '')               AS page_title,
       nullif(jsontext['page_url']::STRING, '')                 AS page_url,
       nullif(jsontext['page_urlfragment']::STRING, '')         AS page_urlfragment,
       nullif(jsontext['page_urlhost']::STRING, '')             AS page_urlhost,
       nullif(jsontext['page_urlpath']::STRING, '')             AS page_urlpath,
       nullif(jsontext['page_urlport']::STRING, '')             AS page_urlport,
       nullif(jsontext['page_urlquery']::STRING, '')            AS page_urlquery,
       nullif(jsontext['page_urlscheme']::STRING, '')           AS page_urlscheme,
       nullif(jsontext['platform']::STRING, '')                 AS platform,
       try_to_numeric(jsontext['pp_xoffset_max']::STRING)       AS pp_xoffset_max,
       try_to_numeric(jsontext['pp_xoffset_min']::STRING)       AS pp_xoffset_min,
       try_to_numeric(jsontext['pp_yoffset_max']::STRING)       AS pp_yoffset_max,
       try_to_numeric(jsontext['pp_yoffset_min']::STRING)       AS pp_yoffset_min,
       nullif(jsontext['refr_domain_userid']::STRING, '')       AS refr_domain_userid,
       nullif(jsontext['refr_dvce_tstamp']::STRING, '')         AS refr_dvce_tstamp,
       nullif(jsontext['refr_medium']::STRING, '')              AS refr_medium,
       nullif(jsontext['refr_source']::STRING, '')              AS refr_source,
       nullif(jsontext['refr_term']::STRING, '')                AS refr_term,
       nullif(jsontext['refr_urlfragment']::STRING, '')         AS refr_urlfragment,
       nullif(jsontext['refr_urlhost']::STRING, '')             AS refr_urlhost,
       nullif(jsontext['refr_urlpath']::STRING, '')             AS refr_urlpath,
       nullif(jsontext['refr_urlport']::STRING, '')             AS refr_urlport,
       nullif(jsontext['refr_urlquery']::STRING, '')            AS refr_urlquery,
       nullif(jsontext['refr_urlscheme']::STRING, '')           AS refr_urlscheme,
       nullif(jsontext['se_action']::STRING, '')                AS se_action,
       nullif(jsontext['se_category']::STRING, '')              AS se_category,
       nullif(jsontext['se_label']::STRING, '')                 AS se_label,
       nullif(jsontext['se_property']::STRING, '')              AS se_property,
       nullif(jsontext['se_value']::STRING, '')                 AS se_value,
       nullif(jsontext['ti_category']::STRING, '')              AS ti_category,
       nullif(jsontext['ti_currency']::STRING, '')              AS ti_currency,
       nullif(jsontext['ti_name']::STRING, '')                  AS ti_name,
       nullif(jsontext['ti_orderid']::STRING, '')               AS ti_orderid,
       nullif(jsontext['ti_price']::STRING, '')                 AS ti_price,
       nullif(jsontext['ti_price_base']::STRING, '')            AS ti_price_base,
       nullif(jsontext['ti_quantity']::STRING, '')              AS ti_quantity,
       nullif(jsontext['ti_sku']::STRING, '')                   AS ti_sku,
       nullif(jsontext['tr_affiliation']::STRING, '')           AS tr_affiliation,
       nullif(jsontext['tr_city']::STRING, '')                  AS tr_city,
       nullif(jsontext['tr_country']::STRING, '')               AS tr_country,
       nullif(jsontext['tr_currency']::STRING, '')              AS tr_currency,
       nullif(jsontext['tr_orderid']::STRING, '')               AS tr_orderid,
       nullif(jsontext['tr_shipping']::STRING, '')              AS tr_shipping,
       nullif(jsontext['tr_shipping_base']::STRING, '')         AS tr_shipping_base,
       nullif(jsontext['tr_state']::STRING, '')                 AS tr_state,
       nullif(jsontext['tr_tax']::STRING, '')                   AS tr_tax,
       nullif(jsontext['tr_tax_base']::STRING, '')              AS tr_tax_base,
       nullif(jsontext['tr_total']::STRING, '')                 AS tr_total,
       nullif(jsontext['tr_total_base']::STRING, '')            AS tr_total_base,
       nullif(jsontext['true_tstamp']::STRING, '')              AS true_tstamp,
       nullif(jsontext['txn_id']::STRING, '')                   AS txn_id,
       CASE
         WHEN event_name IN ('submit_form', 'focus_form', 'change_form')
           THEN 'masked'
         ELSE nullif(jsontext['unstruct_event']::STRING, '')
       END AS unstruct_event,
       nullif(jsontext['user_fingerprint']::STRING, '')         AS user_fingerprint,
       nullif(jsontext['user_id']::STRING, '')                  AS user_id,
       nullif(jsontext['user_ipaddress']::STRING, '')           AS user_ipaddress,
       nullif(jsontext['useragent']::STRING, '')                AS useragent,
       nullif(jsontext['v_collector']::STRING, '')              AS v_collector,
       nullif(jsontext['v_etl']::STRING, '')                    AS v_etl,
       nullif(jsontext['v_tracker']::STRING, '')                AS v_tracker,
       uploaded_at,
       'Fishtown'                                               AS infra_source
{% if target.name not in ("prod") -%}

FROM {{ source('fishtown_snowplow', 'events_sample') }}

{%- else %}

FROM {{ source('fishtown_snowplow', 'events') }}

{%- endif %}

WHERE JSONTEXT['app_id']::string IS NOT NULL
AND date_part(month, jsontext['derived_tstamp']::timestamp) = '{{ month_value }}'
AND date_part(year, jsontext['derived_tstamp']::timestamp) = '{{ year_value }}'
AND lower(JSONTEXT['page_url']::string) NOT LIKE 'https://staging.gitlab.com/%'
AND lower(JSONTEXT['page_url']::string) NOT LIKE 'http://localhost:%'

), events_to_ignore as (

    SELECT event_id
    FROM base
    GROUP BY 1
    HAVING count (*) > 1

), unnested_unstruct as (

    SELECT *,
    {{dbt_utils.get_url_parameter(field='page_urlquery', url_parameter='glm_source')}} AS glm_source,
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
