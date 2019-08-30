{% snapshot sfdc_user_snapshots %}

    {{
        config(
          target_database=env_var("SNOWFLAKE_LOAD_DATABASE"),
          target_schema='snapshots',
          materialized='table', 
          transient=false,
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
        )
    }}
    
    SELECT * 
    FROM {{ source('salesforce', 'user') }}
    
{% endsnapshot %}