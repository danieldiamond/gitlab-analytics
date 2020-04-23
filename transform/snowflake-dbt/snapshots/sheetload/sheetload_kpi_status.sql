{% snapshot sheetload_kpi_status_snapshots %}

    {{
        config(
          unique_key='md5(kpi_grouping || kpi_sub_grouping || kpi)',
          strategy='timestamp',
          updated_at='_UPDATED_AT',
        )
    }}

    SELECT
      MD5(kpi_grouping || kpi_sub_grouping || kpi) AS unique_id,
      *
    FROM {{ source('sheetload','kpi_status') }}

{% endsnapshot %}
