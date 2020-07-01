{{ config({
    "materialized": "table"
    })
}}

{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=[]) %}


WITH usage_data as (

    SELECT * FROM {{ ref('version_usage_data_unpacked') }}

), usage_data_month_base as (

    SELECT
      MD5(usage_data.uuid ||  DATE_TRUNC('month', usage_data.created_at)::DATE)                          AS unique_key,
      MD5(usage_data.uuid || (DATE_TRUNC('month', usage_data.created_at) + INTERVAL '1 month')::DATE)    AS next_unique_key,
      uuid,
      ping_source,
      DATE_TRUNC('month', created_at)::DATE                                                              AS created_at,
      MAX(id)                                                                                            AS ping_id,
      MAX(company)                                                                                       AS company,
      MAX(instance_user_count)                                                                           AS instance_user_count,
      MAX(edition)                                                                                       AS edition,
      MAX(main_edition)                                                                                  AS main_edition,
      MAX(edition_type)                                                                                  AS edition_type,
      MAX(git_version)                                                                                   AS git_version,
      MAX(gitaly_version)                                                                                AS gitaly_version,
      MAX(gitaly_servers)                                                                                AS gitaly_servers,

      {% for ping_name in version_usage_stats_list %}
        MAX({{ping_name}})                                                                               AS {{ping_name}}
          {%- if not loop.last %}      
          ,
          {% endif -%}
      {%- endfor %}
    
    FROM usage_data
    {{ dbt_utils.group_by(n=5) }}
)

SELECT 
  this_month.*,
  CASE
    WHEN next_month.next_unique_key IS NOT NULL THEN FALSE
    ELSE TRUE
   END AS churned_next_month
FROM usage_data_month_base this_month
LEFT JOIN usage_data_month_base AS next_month
  ON this_month.next_unique_key = next_month.unique_key
