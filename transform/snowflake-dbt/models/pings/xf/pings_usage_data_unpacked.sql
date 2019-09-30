{{
  config(
    materialized='incremental',
    unique_key='id'
  )
}}

{% set ping_list = dbt_utils.get_column_values(table=ref('pings_list'), column='full_ping_name', max_records=1000, default=['']) %}

WITH usage_data as (

    SELECT * FROM {{ ref('pings_usage_data') }}

),


final AS (

  SELECT
    id,
    source_ip,
    version,
    installation_type,
    active_user_count,
    created_at,
    mattermost_enabled,
    uuid,
    CASE WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
            THEN 'SaaS'
        ELSE 'Self-Managed' END                                                     AS ping_source,
    edition,
    CONCAT(CONCAT(SPLIT_PART(version, '.', 1), '.'), SPLIT_PART(version, '.', 2))   AS major_version,
    CASE WHEN lower(edition) LIKE '%ee%' THEN 'EE'
      ELSE 'CE' END                                                                 AS main_edition,
    CASE WHEN edition LIKE '%CE%' THEN 'Core'
        WHEN edition LIKE '%EES%' THEN 'Starter'
        WHEN edition LIKE '%EEP%' THEN 'Premium'
        WHEN edition LIKE '%EEU%' THEN 'Ultimate'
        WHEN edition LIKE '%EE Free%' THEN 'Core'
        WHEN edition LIKE '%EE%' THEN 'Starter'
      ELSE NULL END                                                                 AS edition_type,
    hostname,
    host_id,
    git_version,
    gitaly_servers,
    gitaly_version,

    {% for ping_name in ping_list %}
    stats_used['{{ping_name}}']::numeric                                AS {{ping_name}} {{ "," if not loop.last }}
    {% endfor %}

  FROM usage_data

  {% if is_incremental() %}
      WHERE created_at > (SELECT max(created_at) FROM {{ this }})
  {% endif %}

)

SELECT
  *
FROM final
