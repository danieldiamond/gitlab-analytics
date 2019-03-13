with source as (
    
    select * 
    FROM {{ var("database") }}.zendesk_stitch.organizations
    
),

renamed as (
    
    SELECT
        
        --ids
        id                                      as organization_id,
        
        --fields
        name,
        organization_fields:aar                 as arr,
        organization_fields:market_segment      as market_segment,
        organization_fields:salesforce_id       as sfdc_id,

        
        --dates
        created_at,
        updated_at
        
    FROM source
    
)

SELECT * 
FROM renamed