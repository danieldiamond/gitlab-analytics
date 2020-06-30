{% snapshot sheetload_kpi_status_snapshots %}

    {{
        config(
          unique_key='unique_id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}

    SELECT
      MD5(kpi_grouping || kpi_sub_grouping || kpi) AS unique_id,
      {{ dbt_utils.star(from=source('sheetload', 'kpi_status')}},
      _UPDATED_AT::NUMBER::TIMESTAMP as updated_at
    FROM {{ source('sheetload','kpi_status') }}

{% endsnapshot %}
