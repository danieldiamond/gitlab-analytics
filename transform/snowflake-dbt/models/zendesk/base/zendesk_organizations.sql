with source as (
    
    select * 
    FROM {{ var("database") }}.zendesk_stitch.organizations
    
),

renamed as (
    
    SELECT
        
        --ids
        id                                      AS organization_id,
        
        --fields
        name,
        organization_fields:aar                 AS arr,
        organization_fields:market_segment      AS organization_market_segment,
        organization_fields:salesforce_id       AS sfdc_id,

        
        --dates
        created_at,
        updated_at
        
    FROM source
    
)

SELECT organization_id,
    arr,
    sfdc_id,
    {{ sfdc_rename_segment('organization_market_segment', 'organization_market_segment') }} AS organization_market_segment
FROM renamed
