WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_executive_business_review_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
