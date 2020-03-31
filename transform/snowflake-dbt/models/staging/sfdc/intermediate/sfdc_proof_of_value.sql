WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_proof_of_concept_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
AND pov_id NOT IN (
  'a5v4M000001DZYvQAO' -- https://gitlab.com/gitlab-data/analytics/issues/3433
)

