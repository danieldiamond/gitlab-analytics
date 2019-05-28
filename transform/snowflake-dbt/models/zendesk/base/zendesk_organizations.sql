with source as (
    
    SELECT * 
    FROM {{ source('zendesk', 'organizations') }}
),

renamed as (
    
    SELECT
        
        --ids
        id                                                                                  AS organization_id,
        
        --fields
        name,
        organization_fields:aar                                                             AS arr,
        organization_fields:market_segment                                                  AS organization_market_segment,
        REPLACE(organization_fields:salesforce_id, '"', '')                                 AS sfdc_id,
        
        --dates
        created_at,
        updated_at
        
    FROM source
    
)

SELECT * 
FROM renamed