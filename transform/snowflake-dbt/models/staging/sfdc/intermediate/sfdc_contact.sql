WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_contact_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
