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
        CASE WHEN lower(email) LIKE '%gitlab.com%'
                THEN name
            ELSE md5(name)
                END         AS name, --masking folks who are submitting tickets! We don't need to surface that.
        CASE WHEN lower(email) LIKE '%gitlab.com%'
                THEN email
            ELSE md5(email)
                END         AS email, --masking folks who are submitting tickets! We don't need to surface that.
        restricted_agent    AS is_restricted_agent,
        role,
        suspended           AS is_suspended,

        --time
        time_zone,
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed
