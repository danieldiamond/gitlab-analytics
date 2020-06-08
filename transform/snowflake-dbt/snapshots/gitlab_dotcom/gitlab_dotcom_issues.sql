{% snapshot gitlab_dotcom_issues_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    
    WITH source AS (

    	SELECT 
        *
      FROM {{ source('gitlab_dotcom', 'issues') }}
      QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1)
    )
    
    SELECT *
    FROM source
    
{% endsnapshot %}
