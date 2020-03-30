WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_proof_of_concept_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
