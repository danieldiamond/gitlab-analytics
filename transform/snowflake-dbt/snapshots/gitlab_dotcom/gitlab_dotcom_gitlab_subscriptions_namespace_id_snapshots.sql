{% snapshot gitlab_dotcom_gitlab_subscriptions_namespace_snapshots %}

    {{
        config(
          target_database='RAW',
          target_schema='snapshots',
          unique_key='namespace_id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    
    WITH source AS (

      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY updated_at DESC) AS gitlab_subscriptions_rank_in_namespace
      FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions') }}

    )

    SELECT *
    FROM source
    WHERE gitlab_subscriptions_rank_in_namespace = 1
    
{% endsnapshot %}
