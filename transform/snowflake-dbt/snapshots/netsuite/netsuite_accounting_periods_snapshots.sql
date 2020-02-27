{% snapshot netsuite_accounting_periods_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='accounting_period_id',
          updated_at='date_last_modified',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'accounting_periods') }}

{% endsnapshot %}
