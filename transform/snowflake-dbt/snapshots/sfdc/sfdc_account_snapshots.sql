{% snapshot sfdc_account_snapshots %}

    {{
        config(
          target_database='RAW',
          target_schema='snapshots',
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
        )
    }}
    
    SELECT * 
    FROM {{ source('salesforce', 'account') }}
    
{% endsnapshot %}