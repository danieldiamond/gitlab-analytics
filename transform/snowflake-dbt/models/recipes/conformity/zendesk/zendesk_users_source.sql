WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'users') }}

),

renamed AS (

    SELECT  
        id                  AS user_id,

        -- removed external_id,
        organization_id,

        --fields
        email               AS email_address,
        restricted_agent    AS is_restricted_agent,
        role                AS user_role,
        suspended           AS is_suspended,

        --time
        time_zone,
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed
