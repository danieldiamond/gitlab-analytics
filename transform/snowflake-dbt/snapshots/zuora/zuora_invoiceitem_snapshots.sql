{% snapshot zuora_invoiceitem_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora', 'invoiceitem') }}
    
{% endsnapshot %}
