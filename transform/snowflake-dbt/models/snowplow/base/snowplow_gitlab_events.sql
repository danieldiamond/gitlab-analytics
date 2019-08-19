{{config({
    "materialized":"incremental",
    "unique_key":"event_id",
    "schema":"staging"
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
      derived_tstamp,
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
      unstruct_event,
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

WHERE app_id IS NOT NULL
AND lower(page_url) NOT LIKE 'https://staging.gitlab.com/%'
AND lower(page_url) NOT LIKE 'http://localhost:%'
AND event_id not in (
  'd1b9015b-f738-4ae7-a4da-a46523a98f15',
  '8de7b076-120b-42b7-922a-d07faded8c8c',
  '1f820848-2b49-4c01-a721-c9d2a2be77a2',
  '246b20a5-b780-4609-b717-b6f3be18c638', -- https://gitlab.com/gitlab-data/analytics/issues/2165 for context
  'b449dfff-8838-452d-9809-82e25aaac737', -- https://gitlab.com/gitlab-data/analytics/issues/2211
  '5cd32226-d848-49b4-8da2-b0a6a60591e3',
  'b9501cea-a9ac-4d3b-9269-313f574aca5a',
  '8de7b076-120b-42b7-922a-d07faded8c8c',
  'c11c4322-e816-4541-99fb-e9bc4df13b7d',
  '94c324fd-a488-4344-85ba-1125c48db62c',
  'b7020f8c-4c9b-4e00-9fe1-49efdb08ac28',
  '24260041-8135-4280-8603-8b157ee6b643',
  '7d28099c-a976-479c-a3c7-4aeac95ca323',
  '9050a947-9f0e-4616-b653-1343f504fda3',
  '23de1087-364c-485a-aed8-c977a11259b7',
  '98775523-afd9-440e-a498-98292b40e074',
  '09b24fea-f1a9-4f42-aa8e-742837eb85fd',
  'abb83dc0-9b84-4a8d-8ef7-2986ae8d5225',
  'b0b1f5f5-f28f-4318-aba9-fe239d627bdc',
  '6e4000b8-1577-4a92-a376-bd0e2156ea9d',
  '29bbb157-c0f5-4f40-b9c3-13f0cc3e7e13',
  '8e3d8901-6618-4911-aaa8-e601a03cda2d',
  'd1b9015b-f738-4ae7-a4da-a46523a98f15',
  'b7f8ec9d-ad2a-4c55-8c9b-a0f50f02ab10',
  '76f30955-f6ae-47a8-808c-ffa0146c256e',
  'eaff3451-671d-43d7-b3a0-46cf96d72829',
  '84811825-4608-4b60-8c65-90b5397fd864',
  'bd411415-64ae-4434-a0a3-a6e3d72a559c',
  '5b6de40f-48ce-48e4-bb7d-18265c6a5e7c',
  '07d6c1a4-bf8e-4c96-9ed5-c61b9686ab29',
  '6ccb0be7-7443-4ac6-a517-16a508475093',
  '1f820848-2b49-4c01-a721-c9d2a2be77a2',
  '246b20a5-b780-4609-b717-b6f3be18c638',
  '377ce680-0f4c-431d-ab42-5bfe57e3366f',
  '41de7acb-6e54-4433-893e-cd05d9fe6269',
  '2fe303c2-d575-414f-b0a4-2107171144f4',
  'e023e017-33ba-4446-8da5-c9937d5dbb47',
  '31808668-1a73-42d6-b16b-12e129c28d12',
  '37ac7cea-944a-45b7-9340-99d6cbe03552' -- https://gitlab.com/gitlab-data/analytics/issues/2211
  ) 

{% if is_incremental() %}
    AND uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
{% endif %}

), events_to_ignore as (

    SELECT event_id
    FROM base
    GROUP BY 1
    HAVING count (*) > 1

), unnested_unstruct as (

    SELECT *,
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
ORDER BY true_tstamp
