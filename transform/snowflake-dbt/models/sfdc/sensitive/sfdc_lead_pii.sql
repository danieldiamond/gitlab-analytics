WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'lead') }}

), sfdc_lead_pii AS (

    SELECT
        sha1(email) AS email_hash,
        id          AS lead_id,
        email       AS lead_email,
        name        AS lead_name
    FROM source

)

SELECT *
FROM sfdc_lead_pii
