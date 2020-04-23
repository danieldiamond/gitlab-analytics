{% snapshot sheetload_kpi_status_snapshots %}

    {{
        config(
          unique_key='md5(kpi_grouping || kpi_sub_grouping || kpi)',
          strategy='check',
          check_cols='all',
        )
    }}

    SELECT md5(kpi_grouping || kpi_sub_grouping || kpi) as unique_id,
            *
    FROM {{ source('sheetload','kpi_status') }}

{% endsnapshot %}

