{{config({
    "schema": "staging"
  })
}}

WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'opportunity_contact_role') }}

), renamed AS (

    SELECT

      --Primary Key
      id::VARCHAR                       AS opportunity_contact_role_id,

      --Foreign Keys
      contactid::VARCHAR               AS contact_id,
      opportunityid::VARCHAR           AS opportunity_id,
      
      --Info
      isprimary::BOOLEAN                 AS is_primary

    FROM base  

    WHERE isdeleted = FALSE
)

SELECT *
FROM renamed