{% snapshot gitlab_dotcom_gitlab_subscriptions_snapshots %}

    {{
        config(
          target_database='{{ env_var("SNOWFLAKE_LOAD_DATABASE") }}',
          target_schema='snapshots',
          unique_key='gitlab_subscription_id',
          strategy='timestamp',
          updated_at='gitlab_subscription_updated_at',
        )
    }}
    
    SELECT * 
    FROM {{ ref('gitlab_dotcom_gitlab_subscriptions') }}
    
{% endsnapshot %}
