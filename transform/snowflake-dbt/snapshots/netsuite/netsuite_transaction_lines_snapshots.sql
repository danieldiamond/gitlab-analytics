{% snapshot netsuite_transaction_lines_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key=['transaction_line_id', 'transaction_id'],
          updated_at='date_last_modified_gmt',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'transaction_lines') }}

{% endsnapshot %}
