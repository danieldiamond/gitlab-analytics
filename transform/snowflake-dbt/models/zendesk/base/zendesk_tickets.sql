with source as (
    
    SELECT * 
    FROM {{ var("database") }}.zendesk_stitch.tickets
    
), 

renamed as (

    SELECT
        --ids
        id                                              AS ticket_id,
        organization_id,
        assignee_id,
        brand_id,
        group_id,
        requester_id,
        submitter_id,
        
        --fields
        status                                          AS ticket_status,
        priority                                        AS ticket_priority,
        subject                                         AS ticket_subject,
        recipient                                       AS ticket_recipient,
        'type'                                          AS ticket_type,
        url                                             AS api_url,
        -- added ':score'
        REPLACE(satisfaction_rating:score, '"', '')     AS satisfaction_rating_score,

        
        --dates
        created_at                                      AS date_created,
        updated_at                                      AS date_updated
        
    FROM source
    
)

SELECT * 
FROM renamed