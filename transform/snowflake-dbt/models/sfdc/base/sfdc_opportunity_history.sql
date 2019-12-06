WITH base AS (
  
    SELECT *
    FROM {{ source('salesforce', 'opportunity_history') }}


), renamed AS (

    SELECT  amount,
    closedate               AS close_date,
    createdbyid             AS created_by_id,
    createddate             AS created_date,
    expectedrevenue         AS expected_revenue,
    forecastcategory        AS forecast_category,
    id,
    isdeleted               AS is_deleted,
    opportunityid           AS opportunity_id,
    probability,
    stagename               AS stage_name,
    systemmodstamp          AS system_mod_stamp,
    datediff(day, createddate, lead(createddate) OVER (PARTITION BY OPPORTUNITYID ORDER BY CREATEDDATE)) AS days_in_stage
    FROM base

)

SELECT *
FROM renamed
