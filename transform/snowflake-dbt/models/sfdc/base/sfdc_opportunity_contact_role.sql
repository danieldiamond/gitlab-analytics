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
      id::FLOAT                       AS contact_role_id,

      --Foreign Keys
      contactid::VARCHAR               AS contact_id,
      opportunity_id::FLOAT           AS opportunity_id,
      
      --Info
      isprimary::BOOLEAN                 AS is_primary

    FROM base  

    WHERE isdeleted = FALSE
)

SELECT *
FROM renamed