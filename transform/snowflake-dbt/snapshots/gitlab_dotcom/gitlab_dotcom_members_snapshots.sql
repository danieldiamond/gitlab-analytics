{% snapshot gitlab_dotcom_members_snapshots %}

    {{
        config(
          target_database=env_var('SNOWFLAKE_LOAD_DATABASE'),
          target_schema='snapshots',
          unique_key='id',
          strategy='timestamp',
          updated_at='created_at',
        )
    }}
    
    WITH source AS (

    	SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS members_rank_in_key
      
      FROM {{ source('gitlab_dotcom', 'members') }}

    )
    
    SELECT *
    FROM source
    WHERE members_rank_in_key = 1
    
{% endsnapshot %}
