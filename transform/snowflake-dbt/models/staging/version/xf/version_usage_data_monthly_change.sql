{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=[]) %}

WITH mom_change as (

  SELECT
    MD5(uuid || created_at)             AS unique_key,
    uuid,
    created_at,
    ping_source,
    edition,
    main_edition,
    edition_type,
    {% for ping_name in version_usage_stats_list %}
    {{ monthly_change(ping_name) }} {{ "," if not loop.last }}
    {% endfor %}

  FROM {{ ref("version_usage_data_month") }}
  WHERE uuid NOT IN (SELECT uuid FROM {{ ref('version_blacklisted_instance_uuid') }})

)

SELECT *
FROM mom_change
