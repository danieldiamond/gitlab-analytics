{% snapshot netsuite_budget_category_snapshots %}

    {{
        config(
          strategy='check',
          unique_key='budget_category_id',
          check_cols = 'all',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'budget_category') }}

{% endsnapshot %}
