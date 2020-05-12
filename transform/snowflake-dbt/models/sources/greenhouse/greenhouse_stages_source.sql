WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'stages') }}

), renamed as (

	SELECT

            --keys
            id::bigint                  AS stage_id,
            organization_id::bigint     AS organization_id,

            --info
            name::varchar             	AS stage_name,
            "order"::int                AS stage_order,
            active::boolean             AS is_active


	FROM source

), final AS (

    SELECT *,
      IFF(stage_id IN (45 --application_review
                      ,5,6,7,16,27  --Screen
                      ,15,20,46 --Hiring Manager Interview
                      ,18, 30,47,50  --Team Interviews
                      ,8,23,25,29,33,41,43,52,57 --assessment
                      ,22,48 --executive interview
                      ,58,59 --Reference Check    
                      ,53 --background check
                      ,61 --offer
                      ),True, False)               AS is_milestone_stage,
      CASE WHEN stage_id IN (5,6,7,16,27) 
              THEN 'Screen'
           WHEN stage_id IN (18,30,47,50)  
              THEN 'Team Interview - Face to Face'
           WHEN stage_id IN (8,23,25,29,33,41,43,52,57) 
              THEN 'Take Home Assessment'
           WHEN stage_id IN (15,20, 46) 
             THEN 'Hiring Manager Review'
           WHEN stage_id IN (58,59)
             THEN 'Reference Check'
           WHEN stage_id IN (22,48)
             THEN 'Executive Interview'
           ELSE stage_name END::VARCHAR(100)                           AS stage_name_modified 
    FROM renamed
)

SELECT *
FROM final