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
        status                      as ticket_status,
        priority                    as ticket_priority,
        subject                     as ticket_subject,
        recipient                   as ticket_recipient,
        'type'                      as ticket_type,
        url                         as api_url,
        -- added ':score'
        satisfaction_rating:score   as satisfaction_rating_score, 
        
        --dates
        created_at                  as date_created,
        updated_at                  as date_updated
        
    FROM source
    
)

SELECT * 
FROM renamed