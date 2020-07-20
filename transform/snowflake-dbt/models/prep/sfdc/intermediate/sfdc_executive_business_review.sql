WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_executive_business_review_source') }}
    WHERE is_deleted = FALSE

)

SELECT *
FROM source

