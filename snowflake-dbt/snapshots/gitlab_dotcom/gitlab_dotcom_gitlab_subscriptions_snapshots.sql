{% snapshot gitlab_dotcom_gitlab_subscriptions_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    
    WITH source AS (

      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as gitlab_subscriptions_rank_in_key
      FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions') }}

    )

    SELECT *
    FROM source
    WHERE gitlab_subscriptions_rank_in_key = 1
    
{% endsnapshot %}
