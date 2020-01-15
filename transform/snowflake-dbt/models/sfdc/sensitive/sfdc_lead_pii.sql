WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'lead') }}

), sfdc_lead_pii AS (

    SELECT
        id          AS lead_id,
        sha1(email) AS person_id,
        email       AS lead_email,
        name        AS lead_name
    FROM source

)

SELECT *
FROM sfdc_lead_pii
