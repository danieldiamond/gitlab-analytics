{% snapshot gitlab_dotcom_namespaces_snapshots %}

    {{
        config(
          target_database=env_var("SNOWFLAKE_LOAD_DATABASE"),
          target_schema='snapshots',
          unique_key='namespace_id',
          strategy='timestamp',
          updated_at='namespace_updated_at',
        )
    }}
    
    SELECT * 
    FROM {{ ref('gitlab_dotcom_namespaces') }}
    
{% endsnapshot %}
