with source as (
    
    SELECT * 
    FROM {{ var("database") }}.zendesk_stitch.tickets
    
), 

renamed as (

    SELECT
        --ids
        id                          as ticket_id,
        organization_id,
        assignee_id,
        brand_id,
        group_id,
        requester_id,
        submitter_id,
        
        --fields
        status,
        priority,
        subject,
        recipient,
        'type'                      as ticket_type,
        url                         as api_url,
        -- added ':score'
        satisfaction_rating:score   as satisfaction_rating_score, 
        
        --dates
        created_at,
        updated_at
        
    FROM source
    
)

SELECT * 
FROM renamed