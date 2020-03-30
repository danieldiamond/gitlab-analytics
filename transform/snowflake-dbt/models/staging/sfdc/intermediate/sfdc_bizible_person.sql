WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_person_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
