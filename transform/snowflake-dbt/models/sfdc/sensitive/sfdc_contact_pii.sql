WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'contact') }}

), sfdc_contact_pii AS (

    SELECT
        sha1(email) AS email_hash,
        id          AS contact_id,
        email       AS contact_email,
        name        AS contact_name
    FROM source

)

SELECT *
FROM sfdc_contact_pii
