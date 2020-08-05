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
       NULLIF(jsontext['app_id']::VARCHAR, '')                   AS app_id,
       NULLIF(jsontext['base_currency']::VARCHAR, '')            AS base_currency,
       NULLIF(jsontext['br_colordepth']::VARCHAR, '')            AS br_colordepth,
       NULLIF(jsontext['br_cookies']::VARCHAR, '')               AS br_cookies,
       NULLIF(jsontext['br_family']::VARCHAR, '')                AS br_family,
       NULLIF(jsontext['br_features_director']::VARCHAR, '')     AS br_features_director,
       NULLIF(jsontext['br_features_flash']::VARCHAR, '')        AS br_features_flash,
       NULLIF(jsontext['br_features_gears']::VARCHAR, '')        AS br_features_gears,
       NULLIF(jsontext['br_features_java']::VARCHAR, '')         AS br_features_java,
       NULLIF(jsontext['br_features_pdf']::VARCHAR, '')          AS br_features_pdf,
       NULLIF(jsontext['br_features_quicktime']::VARCHAR, '')    AS br_features_quicktime,
       NULLIF(jsontext['br_features_realplayer']::VARCHAR, '')   AS br_features_realplayer,
       NULLIF(jsontext['br_features_silverlight']::VARCHAR, '')  AS br_features_silverlight,
       NULLIF(jsontext['br_features_windowsmedia']::VARCHAR, '') AS br_features_windowsmedia,
       NULLIF(jsontext['br_lang']::VARCHAR, '')                  AS br_lang,
       NULLIF(jsontext['br_name']::VARCHAR, '')                  AS br_name,
       NULLIF(jsontext['br_renderengine']::VARCHAR, '')          AS br_renderengine,
       NULLIF(jsontext['br_type']::VARCHAR, '')                  AS br_type,
       NULLIF(jsontext['br_version']::VARCHAR, '')               AS br_version,
       NULLIF(jsontext['br_viewheight']::VARCHAR, '')            AS br_viewheight,
       NULLIF(jsontext['br_viewwidth']::VARCHAR, '')             AS br_viewwidth,
       NULLIF(jsontext['collector_tstamp']::VARCHAR, '')         AS collector_tstamp,
       NULLIF(jsontext['contexts']::VARCHAR, '')                 AS contexts,
       NULLIF(jsontext['derived_contexts']::VARCHAR, '')         AS derived_contexts,
       NULLIF(jsontext['derived_tstamp']::VARCHAR, '')           AS derived_tstamp,
       NULLIF(jsontext['doc_charset']::VARCHAR, '')              AS doc_charset,
       TRY_TO_NUMERIC(jsontext['doc_height']::VARCHAR)           AS doc_height,
       TRY_TO_NUMERIC(jsontext['doc_width']::VARCHAR)            AS doc_width,
       NULLIF(jsontext['domain_sessionid']::VARCHAR, '')         AS domain_sessionid,
       NULLIF(jsontext['domain_sessionidx']::VARCHAR, '')        AS domain_sessionidx,
       NULLIF(jsontext['domain_userid']::VARCHAR, '')            AS domain_userid,
       NULLIF(jsontext['dvce_created_tstamp']::VARCHAR, '')      AS dvce_created_tstamp,
       NULLIF(jsontext['dvce_ismobile']::VARCHAR, '')            AS dvce_ismobile,
       NULLIF(jsontext['dvce_screenheight']::VARCHAR, '')        AS dvce_screenheight,
       NULLIF(jsontext['dvce_screenwidth']::VARCHAR, '')         AS dvce_screenwidth,
       NULLIF(jsontext['dvce_sent_tstamp']::VARCHAR, '')         AS dvce_sent_tstamp,
       NULLIF(jsontext['dvce_type']::VARCHAR, '')                AS dvce_type,
       NULLIF(jsontext['etl_tags']::VARCHAR, '')                 AS etl_tags,
       NULLIF(jsontext['etl_tstamp']::VARCHAR, '')               AS etl_tstamp,
       NULLIF(jsontext['event']::VARCHAR, '')                    AS event,
       NULLIF(jsontext['event_fingerprint']::VARCHAR, '')        AS event_fingerprint,
       NULLIF(jsontext['event_format']::VARCHAR, '')             AS event_format,
       NULLIF(jsontext['event_id']::VARCHAR, '')                 AS event_id,
       LEFT(RIGHT(jsontext['contexts'], 41), 36)                AS web_page_id,
       NULLIF(jsontext['event_name']::VARCHAR, '')               AS event_name,
       NULLIF(jsontext['event_vendor']::VARCHAR, '')             AS event_vendor,
       NULLIF(jsontext['event_version']::VARCHAR, '')            AS event_version,
       NULLIF(jsontext['geo_city']::VARCHAR, '')                 AS geo_city,
       NULLIF(jsontext['geo_country']::VARCHAR, '')              AS geo_country,
       NULLIF(jsontext['geo_latitude']::VARCHAR, '')             AS geo_latitude,
       NULLIF(jsontext['geo_longitude']::VARCHAR, '')            AS geo_longitude,
       NULLIF(jsontext['geo_region']::VARCHAR, '')               AS geo_region,
       NULLIF(jsontext['geo_region_name']::VARCHAR, '')          AS geo_region_name,
       NULLIF(jsontext['geo_timezone']::VARCHAR, '')             AS geo_timezone,
       NULLIF(jsontext['geo_zipcode']::VARCHAR, '')              AS geo_zipcode,
       NULLIF(jsontext['ip_domain']::VARCHAR, '')                AS ip_domain,
       NULLIF(jsontext['ip_isp']::VARCHAR, '')                   AS ip_isp,
       NULLIF(jsontext['ip_netspeed']::VARCHAR, '')              AS ip_netspeed,
       NULLIF(jsontext['ip_organization']::VARCHAR, '')          AS ip_organization,
       NULLIF(jsontext['mkt_campaign']::VARCHAR, '')             AS mkt_campaign,
       NULLIF(jsontext['mkt_clickid']::VARCHAR, '')              AS mkt_clickid,
       NULLIF(jsontext['mkt_content']::VARCHAR, '')              AS mkt_content,
       NULLIF(jsontext['mkt_medium']::VARCHAR, '')               AS mkt_medium,
       NULLIF(jsontext['mkt_network']::VARCHAR, '')              AS mkt_network,
       NULLIF(jsontext['mkt_source']::VARCHAR, '')               AS mkt_source,
       NULLIF(jsontext['mkt_term']::VARCHAR, '')                 AS mkt_term,
       NULLIF(jsontext['name_tracker']::VARCHAR, '')             AS name_tracker,
       NULLIF(jsontext['network_userid']::VARCHAR, '')           AS network_userid,
       NULLIF(jsontext['os_family']::VARCHAR, '')                AS os_family,
       NULLIF(jsontext['os_manufacturer']::VARCHAR, '')          AS os_manufacturer,
       NULLIF(jsontext['os_name']::VARCHAR, '')                  AS os_name,
       NULLIF(jsontext['os_timezone']::VARCHAR, '')              AS os_timezone,
       NULLIF(jsontext['page_referrer']::VARCHAR, '')            AS page_referrer,
       NULLIF(jsontext['page_title']::VARCHAR, '')               AS page_title,
       NULLIF(jsontext['page_url']::VARCHAR, '')                 AS page_url,
       NULLIF(jsontext['page_urlfragment']::VARCHAR, '')         AS page_urlfragment,
       NULLIF(jsontext['page_urlhost']::VARCHAR, '')             AS page_urlhost,
       NULLIF(jsontext['page_urlpath']::VARCHAR, '')             AS page_urlpath,
       NULLIF(jsontext['page_urlport']::VARCHAR, '')             AS page_urlport,
       NULLIF(jsontext['page_urlquery']::VARCHAR, '')            AS page_urlquery,
       NULLIF(jsontext['page_urlscheme']::VARCHAR, '')           AS page_urlscheme,
       NULLIF(jsontext['platform']::VARCHAR, '')                 AS platform,
       TRY_TO_NUMERIC(jsontext['pp_xoffset_max']::VARCHAR)       AS pp_xoffset_max,
       TRY_TO_NUMERIC(jsontext['pp_xoffset_min']::VARCHAR)       AS pp_xoffset_min,
       TRY_TO_NUMERIC(jsontext['pp_yoffset_max']::VARCHAR)       AS pp_yoffset_max,
       TRY_TO_NUMERIC(jsontext['pp_yoffset_min']::VARCHAR)       AS pp_yoffset_min,
       NULLIF(jsontext['refr_domain_userid']::VARCHAR, '')       AS refr_domain_userid,
       NULLIF(jsontext['refr_dvce_tstamp']::VARCHAR, '')         AS refr_dvce_tstamp,
       NULLIF(jsontext['refr_medium']::VARCHAR, '')              AS refr_medium,
       NULLIF(jsontext['refr_source']::VARCHAR, '')              AS refr_source,
       NULLIF(jsontext['refr_term']::VARCHAR, '')                AS refr_term,
       NULLIF(jsontext['refr_urlfragment']::VARCHAR, '')         AS refr_urlfragment,
       NULLIF(jsontext['refr_urlhost']::VARCHAR, '')             AS refr_urlhost,
       NULLIF(jsontext['refr_urlpath']::VARCHAR, '')             AS refr_urlpath,
       NULLIF(jsontext['refr_urlport']::VARCHAR, '')             AS refr_urlport,
       NULLIF(jsontext['refr_urlquery']::VARCHAR, '')            AS refr_urlquery,
       NULLIF(jsontext['refr_urlscheme']::VARCHAR, '')           AS refr_urlscheme,
       NULLIF(jsontext['se_action']::VARCHAR, '')                AS se_action,
       NULLIF(jsontext['se_category']::VARCHAR, '')              AS se_category,
       NULLIF(jsontext['se_label']::VARCHAR, '')                 AS se_label,
       NULLIF(jsontext['se_property']::VARCHAR, '')              AS se_property,
       NULLIF(jsontext['se_value']::VARCHAR, '')                 AS se_value,
       NULLIF(jsontext['ti_category']::VARCHAR, '')              AS ti_category,
       NULLIF(jsontext['ti_currency']::VARCHAR, '')              AS ti_currency,
       NULLIF(jsontext['ti_name']::VARCHAR, '')                  AS ti_name,
       NULLIF(jsontext['ti_orderid']::VARCHAR, '')               AS ti_orderid,
       NULLIF(jsontext['ti_price']::VARCHAR, '')                 AS ti_price,
       NULLIF(jsontext['ti_price_base']::VARCHAR, '')            AS ti_price_base,
       NULLIF(jsontext['ti_quantity']::VARCHAR, '')              AS ti_quantity,
       NULLIF(jsontext['ti_sku']::VARCHAR, '')                   AS ti_sku,
       NULLIF(jsontext['tr_affiliation']::VARCHAR, '')           AS tr_affiliation,
       NULLIF(jsontext['tr_city']::VARCHAR, '')                  AS tr_city,
       NULLIF(jsontext['tr_country']::VARCHAR, '')               AS tr_country,
       NULLIF(jsontext['tr_currency']::VARCHAR, '')              AS tr_currency,
       NULLIF(jsontext['tr_orderid']::VARCHAR, '')               AS tr_orderid,
       NULLIF(jsontext['tr_shipping']::VARCHAR, '')              AS tr_shipping,
       NULLIF(jsontext['tr_shipping_base']::VARCHAR, '')         AS tr_shipping_base,
       NULLIF(jsontext['tr_state']::VARCHAR, '')                 AS tr_state,
       NULLIF(jsontext['tr_tax']::VARCHAR, '')                   AS tr_tax,
       NULLIF(jsontext['tr_tax_base']::VARCHAR, '')              AS tr_tax_base,
       NULLIF(jsontext['tr_total']::VARCHAR, '')                 AS tr_total,
       NULLIF(jsontext['tr_total_base']::VARCHAR, '')            AS tr_total_base,
       NULLIF(jsontext['true_tstamp']::VARCHAR, '')              AS true_tstamp,
       NULLIF(jsontext['txn_id']::VARCHAR, '')                   AS txn_id,
       CASE
         WHEN event_name IN ('submit_form', 'focus_form', 'change_form')
           THEN 'masked'
         ELSE NULLIF(jsontext['unstruct_event']::VARCHAR, '')
       END AS unstruct_event,
       NULLIF(jsontext['user_fingerprint']::VARCHAR, '')         AS user_fingerprint,
       NULLIF(jsontext['user_id']::VARCHAR, '')                  AS user_id,
       NULLIF(jsontext['user_ipaddress']::VARCHAR, '')           AS user_ipaddress,
       NULLIF(jsontext['useragent']::VARCHAR, '')                AS useragent,
       NULLIF(jsontext['v_collector']::VARCHAR, '')              AS v_collector,
       NULLIF(jsontext['v_etl']::VARCHAR, '')                    AS v_etl,
       NULLIF(jsontext['v_tracker']::VARCHAR, '')                AS v_tracker,
       uploaded_at,
       'Fishtown'                                               AS infra_source
{% if target.name not in ("prod") -%}

FROM {{ ref('snowplow_fishtown_good_events_sample_source') }}

{%- else %}

FROM {{ ref('snowplow_fishtown_good_events_source') }}

{%- endif %}

WHERE JSONTEXT['app_id']::VARCHAR IS NOT NULL
AND DATE_PART(month, jsontext['derived_tstamp']::timestamp) = '{{ month_value }}'
AND DATE_PART(year, jsontext['derived_tstamp']::timestamp) = '{{ year_value }}'
AND LOWER(JSONTEXT['page_url']::VARCHAR) NOT LIKE 'https://staging.gitlab.com/%'
AND LOWER(JSONTEXT['page_url']::VARCHAR) NOT LIKE 'http://localhost:%'

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
