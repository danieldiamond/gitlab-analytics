WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_person_source') }}
    WHERE is_deleted = FALSE

)

SELECT *
FROM source

