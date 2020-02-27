{% snapshot netsuite_consolidated_exchange_rates_snapshots %}

    {{
        config(
          strategy='check',
          unique_key='consolidated_exchange_rate_id',
          check_cols = 'all'
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'consolidated_exchange_rates') }}

{% endsnapshot %}
