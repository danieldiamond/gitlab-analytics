{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH base AS (
  
    SELECT *
    FROM {{ var("database") }}.salesforce_stitch.opportunityhistory

), final AS (

    SELECT  *,
          datediff(day, createddate, 
            lead(createddate) OVER (PARTITION BY OPPORTUNITYID ORDER BY CREATEDDATE)) AS days_in_stage
    FROM base

)

SELECT * 
FROM final