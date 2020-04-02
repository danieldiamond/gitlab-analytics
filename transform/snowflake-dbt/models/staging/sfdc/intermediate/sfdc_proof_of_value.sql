WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_proof_of_concept_source') }}
    WHERE is_deleted = FALSE

)

SELECT *
FROM source

