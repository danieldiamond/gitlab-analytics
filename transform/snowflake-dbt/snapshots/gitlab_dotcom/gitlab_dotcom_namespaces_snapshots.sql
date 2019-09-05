{% snapshot gitlab_dotcom_namespaces_snapshots %}

    {{
        config(
          target_database='RAW',
          target_schema='snapshots',
          unique_key='namespace_id',
          strategy='timestamp',
          updated_at='namespace_updated_at',
        )
    }}
    
    SELECT * 
    FROM {{ ref('gitlab_dotcom_namespaces') }}
    
{% endsnapshot %}
