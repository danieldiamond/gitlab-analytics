{{ config({
    "schema": "staging"
    })
}}

{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000) %}


WITH usage_data as (

    SELECT * FROM {{ ref('version_usage_data_unpacked') }}

), usage_data_month_base as (

    SELECT  md5(usage_data.uuid || date_trunc('month', usage_data.created_at)::date)                         AS unique_key,
            md5(usage_data.uuid || (date_trunc('month', usage_data.created_at) + INTERVAL '1 month') ::date) AS next_unique_key,
            uuid,
            ping_source,
            DATE_TRUNC('month', created_at)::date                                                            AS created_at,
            max(id)                                                                                          AS ping_id,
            max(active_user_count)                                                                           AS active_user_count,
            max(edition)                                                                                     AS edition,
            max(main_edition)                                                                                AS main_edition,
            max(edition_type)                                                                                AS edition_type,
            max(git_version)                                                                                 AS git_version,
            max(gitaly_version)                                                                              AS gitaly_version,
            max(gitaly_servers)                                                                              AS gitaly_servers,

            {% for ping_name in ping_list %}
            max({{ping_name}})                                                                               AS {{ping_name}} {{ "," if not loop.last }}
            {%- endfor -%}

    FROM usage_data
    {{ dbt_utils.group_by(n=5) }}
)

SELECT  this_month.*,
        CASE WHEN next_month.next_unique_key IS NOT NULL
                THEN FALSE
             ELSE TRUE
        END AS churned_next_month
FROM usage_data_month_base this_month
LEFT JOIN usage_data_month_base next_month
  ON this_month.next_unique_key = next_month.unique_key
