-- depends_on: {{ ref('pings_usage_data') }}

{{get_pings_json_keys()}}


WITH usage_data as (

    SELECT * FROM {{ ref('pings_usage_data_unpacked') }}

), usage_data_month_base as (

    SELECT  md5(usage_data.uuid || date_trunc('month', usage_data.created_at)::date)                         AS unique_key,
            md5(usage_data.uuid || (date_trunc('month', usage_data.created_at) + INTERVAL '1 month') ::date) AS next_unique_key,
            uuid                                                                                             AS uuid,
            DATE_TRUNC('month', created_at)                                                                  AS created_at,
            max(id)                                                                                          AS ping_id,
            max(active_user_count)                                                                           AS active_user_count,
            max(edition)                                                                                     AS edition,
            max(main_edition)                                                                                AS main_edition,
            max(edition_type)                                                                                AS edition_type,

            {%- for row in load_result('stats_used')['data'] -%}
            max({{row[1]}})                                                                                  AS {{row[1]}} {{ "," if not loop.last }}
            {%- endfor -%}

    FROM usage_data
    GROUP BY 1, 2, 3, 4
)

SELECT  this_month.*,
        CASE WHEN next_month.next_unique_key IS NOT NULL
                THEN FALSE
             ELSE TRUE
        END AS churned_next_month,
        CASE WHEN this_month.uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
                THEN 'SaaS'
             ELSE 'Self-Hosted'
        END AS ping_source
FROM usage_data_month_base this_month
LEFT JOIN usage_data_month_base next_month
  ON this_month.next_unique_key = next_month.unique_key
