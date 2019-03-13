with source as (

    SELECT * 
    FROM {{ var("database") }}.zendesk_stitch.users
    
),

renamed AS (
    
    SELECT
        
        --ids
        id                      as user_id,
        
        -- removed external_id,
        organization_id,
        
        --fields
        name,
        email,
        restricted_agent,
        role,
        suspended,
        
        --dates
        created_at,
        last_login_at,
        updated_at
        
    FROM source

)

SELECT * 
FROM renamed
