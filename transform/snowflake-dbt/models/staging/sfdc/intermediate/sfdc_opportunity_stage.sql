WITH base AS (

    SELECT *
    FROM {{ source('sfdc_opportunity_stage_source') }}

)

SELECT *
FROM base

