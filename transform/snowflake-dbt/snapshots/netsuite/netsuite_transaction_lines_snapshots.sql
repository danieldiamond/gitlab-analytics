{% snapshot netsuite_transaction_lines_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='date_last_modified_gmt',
        )
    }}

    SELECT concat(transaction_id, transaction_line_id) as id,
           *
    FROM {{ source('netsuite', 'transaction_lines') }}

{% endsnapshot %}
