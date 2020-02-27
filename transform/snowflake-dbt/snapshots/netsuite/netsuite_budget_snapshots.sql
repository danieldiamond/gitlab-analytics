{% snapshot netsuite_budget_snapshots %}

    {{
        config(
          strategy='check',
          unique_key='budget_id',
          check_cols = 'all',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'budget') }}

{% endsnapshot %}
