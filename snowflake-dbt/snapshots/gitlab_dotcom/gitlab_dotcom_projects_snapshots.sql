{% snapshot gitlab_dotcom_projects_snapshots %}

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
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as projects_rank_in_key
      
      FROM {{ source('gitlab_dotcom', 'projects') }}

    )
    
    SELECT *
    FROM source
    WHERE projects_rank_in_key = 1
        
{% endsnapshot %}
