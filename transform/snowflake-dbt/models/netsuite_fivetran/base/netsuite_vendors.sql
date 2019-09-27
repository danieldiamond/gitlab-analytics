{{ config({
    "materialized": "ephemeral"
    })
}}

with source AS (

    SELECT *
    FROM {{ source('netsuite', 'vendors') }}

), renamed AS (

    SELECT vendor_id,
           companyname               AS vendor_name,
           currency_id,
           represents_subsidiary_id  AS subsidiary_id,
           openbalance               AS vendor_balance,
           comments                  AS vendor_comments,
           is1099eligible::boolean   AS is_1099_eligible,
           isinactive::boolean       AS is_inactive,
           is_person::boolean        AS is_person

    FROM source

)

SELECT *
FROM renamed

--We no longer have first and last names for folks who are paid by contracts.
