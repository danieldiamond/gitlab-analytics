-- depends_on: {{ ref('pings_usage_data') }}
{{
  config(
    materialized='incremental',
    unique_key='id', 
    schema='analytics'
  )
}}

{{get_pings_json_keys()}}

WITH usage_data as (

    SELECT * FROM {{ ref('pings_usage_data') }}
)

SELECT  id,
        source_ip,
        version,
        installation_type,
        active_user_count,
        created_at,
        mattermost_enabled,
        uuid,
        edition,
        CASE
          WHEN version LIKE '%ee%'
            THEN 'EE'
          ELSE 'CE'
        END                                                                            AS main_edition,
        CASE
          WHEN edition LIKE '%CE%'
            THEN 'Core'
          WHEN edition LIKE '%EES%'
            THEN 'Starter'
          WHEN edition LIKE '%EEP%'
            THEN 'Premium'
          WHEN edition LIKE '%EEU%'
            THEN 'Ultimate'
          WHEN edition LIKE '%EE Free%'
            THEN 'Core'
          WHEN edition LIKE '%EE%'
            THEN 'Starter'
          ELSE null
        END                                                                             AS edition_type,
        hostname,
        host_id,

        {%- for row in load_result('stats_used')['data'] -%}
        stats_used:{{row[0]}}::numeric                                                  AS {{row[1]}},
        {%- endfor -%}

        concat(concat(SPLIT_PART(version, '.', 1), '.'), SPLIT_PART(version, '.', 2))   AS major_version
FROM usage_data
{% if is_incremental() %}
    WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
