{% snapshot netsuite_transaction_lines_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='transaction_line_id',
          updated_at='date_last_modified',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'transactions') }}

{% endsnapshot %}
