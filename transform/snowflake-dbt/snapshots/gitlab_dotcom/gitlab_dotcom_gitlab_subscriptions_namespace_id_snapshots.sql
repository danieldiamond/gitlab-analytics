{% snapshot gitlab_dotcom_gitlab_subscriptions_namespace_id_snapshots %}

    {{
        config(
          target_database='RAW',
          target_schema='snapshots',
          unique_key='namespace_id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    
    SELECT *
    FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1 
    
{% endsnapshot %}