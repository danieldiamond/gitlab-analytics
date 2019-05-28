with source as (

    SELECT * 
    FROM {{ source('zendesk', 'users') }}

    
),

renamed as (
    
    SELECT
        
        --ids
        id                                                                                  AS user_id,
        
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
