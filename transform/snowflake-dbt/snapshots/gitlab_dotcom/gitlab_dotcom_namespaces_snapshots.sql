{% snapshot gitlab_dotcom_namespaces_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    
    WITH source as (

    	SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS namespaces_rank_in_key
      
      FROM {{ source('gitlab_dotcom', 'namespaces') }}

    )
    
    SELECT *
    FROM source
    WHERE namespaces_rank_in_key = 1
    
{% endsnapshot %}
