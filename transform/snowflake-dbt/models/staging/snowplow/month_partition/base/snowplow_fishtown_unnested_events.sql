{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "unique_key":"event_id"
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
       NULLIF(jsontext['app_id']::STRING, '')                   AS app_id,
       NULLIF(jsontext['base_currency']::STRING, '')            AS base_currency,
       NULLIF(jsontext['br_colordepth']::STRING, '')            AS br_colordepth,
       NULLIF(jsontext['br_cookies']::STRING, '')               AS br_cookies,
       NULLIF(jsontext['br_family']::STRING, '')                AS br_family,
       NULLIF(jsontext['br_features_director']::STRING, '')     AS br_features_director,
       NULLIF(jsontext['br_features_flash']::STRING, '')        AS br_features_flash,
       NULLIF(jsontext['br_features_gears']::STRING, '')        AS br_features_gears,
       NULLIF(jsontext['br_features_java']::STRING, '')         AS br_features_java,
       NULLIF(jsontext['br_features_pdf']::STRING, '')          AS br_features_pdf,
       NULLIF(jsontext['br_features_quicktime']::STRING, '')    AS br_features_quicktime,
       NULLIF(jsontext['br_features_realplayer']::STRING, '')   AS br_features_realplayer,
       NULLIF(jsontext['br_features_silverlight']::STRING, '')  AS br_features_silverlight,
       NULLIF(jsontext['br_features_windowsmedia']::STRING, '') AS br_features_windowsmedia,
       NULLIF(jsontext['br_lang']::STRING, '')                  AS br_lang,
       NULLIF(jsontext['br_name']::STRING, '')                  AS br_name,
       NULLIF(jsontext['br_renderengine']::STRING, '')          AS br_renderengine,
       NULLIF(jsontext['br_type']::STRING, '')                  AS br_type,
       NULLIF(jsontext['br_version']::STRING, '')               AS br_version,
       NULLIF(jsontext['br_viewheight']::STRING, '')            AS br_viewheight,
       NULLIF(jsontext['br_viewwidth']::STRING, '')             AS br_viewwidth,
       NULLIF(jsontext['collector_tstamp']::STRING, '')         AS collector_tstamp,
       NULLIF(jsontext['contexts']::STRING, '')                 AS contexts,
       NULLIF(jsontext['derived_contexts']::STRING, '')         AS derived_contexts,
       NULLIF(jsontext['derived_tstamp']::STRING, '')           AS derived_tstamp,
       NULLIF(jsontext['doc_charset']::STRING, '')              AS doc_charset,
       TRY_TO_NUMERIC(jsontext['doc_height']::STRING)           AS doc_height,
       TRY_TO_NUMERIC(jsontext['doc_width']::STRING)            AS doc_width,
       NULLIF(jsontext['domain_sessionid']::STRING, '')         AS domain_sessionid,
       NULLIF(jsontext['domain_sessionidx']::STRING, '')        AS domain_sessionidx,
       NULLIF(jsontext['domain_userid']::STRING, '')            AS domain_userid,
       NULLIF(jsontext['dvce_created_tstamp']::STRING, '')      AS dvce_created_tstamp,
       NULLIF(jsontext['dvce_ismobile']::STRING, '')            AS dvce_ismobile,
       NULLIF(jsontext['dvce_screenheight']::STRING, '')        AS dvce_screenheight,
       NULLIF(jsontext['dvce_screenwidth']::STRING, '')         AS dvce_screenwidth,
       NULLIF(jsontext['dvce_sent_tstamp']::STRING, '')         AS dvce_sent_tstamp,
       NULLIF(jsontext['dvce_type']::STRING, '')                AS dvce_type,
       NULLIF(jsontext['etl_tags']::STRING, '')                 AS etl_tags,
       NULLIF(jsontext['etl_tstamp']::STRING, '')               AS etl_tstamp,
       NULLIF(jsontext['event']::STRING, '')                    AS event,
       NULLIF(jsontext['event_fingerprint']::STRING, '')        AS event_fingerprint,
       NULLIF(jsontext['event_format']::STRING, '')             AS event_format,
       NULLIF(jsontext['event_id']::STRING, '')                 AS event_id,
       LEFT(RIGHT(jsontext['contexts'], 41), 36)                AS web_page_id,
       NULLIF(jsontext['event_name']::STRING, '')               AS event_name,
       NULLIF(jsontext['event_vendor']::STRING, '')             AS event_vendor,
       NULLIF(jsontext['event_version']::STRING, '')            AS event_version,
       NULLIF(jsontext['geo_city']::STRING, '')                 AS geo_city,
       NULLIF(jsontext['geo_country']::STRING, '')              AS geo_country,
       NULLIF(jsontext['geo_latitude']::STRING, '')             AS geo_latitude,
       NULLIF(jsontext['geo_longitude']::STRING, '')            AS geo_longitude,
       NULLIF(jsontext['geo_region']::STRING, '')               AS geo_region,
       NULLIF(jsontext['geo_region_name']::STRING, '')          AS geo_region_name,
       NULLIF(jsontext['geo_timezone']::STRING, '')             AS geo_timezone,
       NULLIF(jsontext['geo_zipcode']::STRING, '')              AS geo_zipcode,
       NULLIF(jsontext['ip_domain']::STRING, '')                AS ip_domain,
       NULLIF(jsontext['ip_isp']::STRING, '')                   AS ip_isp,
       NULLIF(jsontext['ip_netspeed']::STRING, '')              AS ip_netspeed,
       NULLIF(jsontext['ip_organization']::STRING, '')          AS ip_organization,
       NULLIF(jsontext['mkt_campaign']::STRING, '')             AS mkt_campaign,
       NULLIF(jsontext['mkt_clickid']::STRING, '')              AS mkt_clickid,
       NULLIF(jsontext['mkt_content']::STRING, '')              AS mkt_content,
       NULLIF(jsontext['mkt_medium']::STRING, '')               AS mkt_medium,
       NULLIF(jsontext['mkt_network']::STRING, '')              AS mkt_network,
       NULLIF(jsontext['mkt_source']::STRING, '')               AS mkt_source,
       NULLIF(jsontext['mkt_term']::STRING, '')                 AS mkt_term,
       NULLIF(jsontext['name_tracker']::STRING, '')             AS name_tracker,
       NULLIF(jsontext['network_userid']::STRING, '')           AS network_userid,
       NULLIF(jsontext['os_family']::STRING, '')                AS os_family,
       NULLIF(jsontext['os_manufacturer']::STRING, '')          AS os_manufacturer,
       NULLIF(jsontext['os_name']::STRING, '')                  AS os_name,
       NULLIF(jsontext['os_timezone']::STRING, '')              AS os_timezone,
       NULLIF(jsontext['page_referrer']::STRING, '')            AS page_referrer,
       NULLIF(jsontext['page_title']::STRING, '')               AS page_title,
       NULLIF(jsontext['page_url']::STRING, '')                 AS page_url,
       NULLIF(jsontext['page_urlfragment']::STRING, '')         AS page_urlfragment,
       NULLIF(jsontext['page_urlhost']::STRING, '')             AS page_urlhost,
       NULLIF(jsontext['page_urlpath']::STRING, '')             AS page_urlpath,
       NULLIF(jsontext['page_urlport']::STRING, '')             AS page_urlport,
       NULLIF(jsontext['page_urlquery']::STRING, '')            AS page_urlquery,
       NULLIF(jsontext['page_urlscheme']::STRING, '')           AS page_urlscheme,
       NULLIF(jsontext['platform']::STRING, '')                 AS platform,
       TRY_TO_NUMERIC(jsontext['pp_xoffset_max']::STRING)       AS pp_xoffset_max,
       TRY_TO_NUMERIC(jsontext['pp_xoffset_min']::STRING)       AS pp_xoffset_min,
       TRY_TO_NUMERIC(jsontext['pp_yoffset_max']::STRING)       AS pp_yoffset_max,
       TRY_TO_NUMERIC(jsontext['pp_yoffset_min']::STRING)       AS pp_yoffset_min,
       NULLIF(jsontext['refr_domain_userid']::STRING, '')       AS refr_domain_userid,
       NULLIF(jsontext['refr_dvce_tstamp']::STRING, '')         AS refr_dvce_tstamp,
       NULLIF(jsontext['refr_medium']::STRING, '')              AS refr_medium,
       NULLIF(jsontext['refr_source']::STRING, '')              AS refr_source,
       NULLIF(jsontext['refr_term']::STRING, '')                AS refr_term,
       NULLIF(jsontext['refr_urlfragment']::STRING, '')         AS refr_urlfragment,
       NULLIF(jsontext['refr_urlhost']::STRING, '')             AS refr_urlhost,
       NULLIF(jsontext['refr_urlpath']::STRING, '')             AS refr_urlpath,
       NULLIF(jsontext['refr_urlport']::STRING, '')             AS refr_urlport,
       NULLIF(jsontext['refr_urlquery']::STRING, '')            AS refr_urlquery,
       NULLIF(jsontext['refr_urlscheme']::STRING, '')           AS refr_urlscheme,
       NULLIF(jsontext['se_action']::STRING, '')                AS se_action,
       NULLIF(jsontext['se_category']::STRING, '')              AS se_category,
       NULLIF(jsontext['se_label']::STRING, '')                 AS se_label,
       NULLIF(jsontext['se_property']::STRING, '')              AS se_property,
       NULLIF(jsontext['se_value']::STRING, '')                 AS se_value,
       NULLIF(jsontext['ti_category']::STRING, '')              AS ti_category,
       NULLIF(jsontext['ti_currency']::STRING, '')              AS ti_currency,
       NULLIF(jsontext['ti_name']::STRING, '')                  AS ti_name,
       NULLIF(jsontext['ti_orderid']::STRING, '')               AS ti_orderid,
       NULLIF(jsontext['ti_price']::STRING, '')                 AS ti_price,
       NULLIF(jsontext['ti_price_base']::STRING, '')            AS ti_price_base,
       NULLIF(jsontext['ti_quantity']::STRING, '')              AS ti_quantity,
       NULLIF(jsontext['ti_sku']::STRING, '')                   AS ti_sku,
       NULLIF(jsontext['tr_affiliation']::STRING, '')           AS tr_affiliation,
       NULLIF(jsontext['tr_city']::STRING, '')                  AS tr_city,
       NULLIF(jsontext['tr_country']::STRING, '')               AS tr_country,
       NULLIF(jsontext['tr_currency']::STRING, '')              AS tr_currency,
       NULLIF(jsontext['tr_orderid']::STRING, '')               AS tr_orderid,
       NULLIF(jsontext['tr_shipping']::STRING, '')              AS tr_shipping,
       NULLIF(jsontext['tr_shipping_base']::STRING, '')         AS tr_shipping_base,
       NULLIF(jsontext['tr_state']::STRING, '')                 AS tr_state,
       NULLIF(jsontext['tr_tax']::STRING, '')                   AS tr_tax,
       NULLIF(jsontext['tr_tax_base']::STRING, '')              AS tr_tax_base,
       NULLIF(jsontext['tr_total']::STRING, '')                 AS tr_total,
       NULLIF(jsontext['tr_total_base']::STRING, '')            AS tr_total_base,
       NULLIF(jsontext['true_tstamp']::STRING, '')              AS true_tstamp,
       NULLIF(jsontext['txn_id']::STRING, '')                   AS txn_id,
       CASE
         WHEN event_name IN ('submit_form', 'focus_form', 'change_form')
           THEN 'masked'
         ELSE NULLIF(jsontext['unstruct_event']::STRING, '')
       END AS unstruct_event,
       NULLIF(jsontext['user_fingerprint']::STRING, '')         AS user_fingerprint,
       NULLIF(jsontext['user_id']::STRING, '')                  AS user_id,
       NULLIF(jsontext['user_ipaddress']::STRING, '')           AS user_ipaddress,
       NULLIF(jsontext['useragent']::STRING, '')                AS useragent,
       NULLIF(jsontext['v_collector']::STRING, '')              AS v_collector,
       NULLIF(jsontext['v_etl']::STRING, '')                    AS v_etl,
       NULLIF(jsontext['v_tracker']::STRING, '')                AS v_tracker,
       uploaded_at,
       'Fishtown'                                               AS infra_source
{% if target.name not in ("prod") -%}

FROM {{ ref('snowplow_fishtown_good_events_sample_source') }}

{%- else %}

FROM {{ ref('snowplow_fishtown_good_events_source') }}

{%- endif %}

WHERE JSONTEXT['app_id']::string IS NOT NULL
AND DATE_PART(month, jsontext['derived_tstamp']::timestamp) = '{{ month_value }}'
AND DATE_PART(year, jsontext['derived_tstamp']::timestamp) = '{{ year_value }}'
AND LOWER(JSONTEXT['page_url']::string) NOT LIKE 'https://staging.gitlab.com/%'
AND LOWER(JSONTEXT['page_url']::string) NOT LIKE 'http://localhost:%'

), events_to_ignore as (

    SELECT event_id
    FROM base
    GROUP BY 1
    HAVING count (*) > 1

), unnested_unstruct as (

    SELECT *,
    {{dbt_utils.get_url_parameter(field='page_urlquery', url_parameter='glm_source')}} AS glm_source,
    CASE
      WHEN LENGTH(unstruct_event) > 0 AND try_parse_json(unstruct_event) IS NULL
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
