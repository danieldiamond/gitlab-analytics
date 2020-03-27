WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}

)

SELECT *
FROM base
 WHERE account_id IS NOT NULL
      AND is_deleted = FALSE
