{% snapshot netsuite_budget_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='budget_category_id',
          check_cols = 'all',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'budget_category') }}

{% endsnapshot %}
