WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'application_stages') }}

), renamed as (

	SELECT
    		--keys
    		application_id::bigint		    AS application_id,
    		stage_id::bigint			    AS stage_id,

    		--info
    		entered_on::timestamp 		    AS stage_entered_on,
    		exited_on::timestamp 		    AS stage_exited_on,
    		stage_name::varchar 		    AS application_stage_name

	FROM source

), intermediate AS (

    SELECT 
      renamed.*,
      IFF(stage_id IN (45 --application_review
                   ,7 --screen
                   ,8 -- Screen
                   ,26 --screening
                   ,22 --HM Interview
                   ,46 --HIring Manager Interview
                   ,23 --Team Interviews
                   ,47 --Team Interview    
                   ,33 --face to face
                   ,48 --executive interview
                   ,58 --references
                   ,59 --Reference Check    
                   ,53 --background check
                   ,61 --offer
                   ),True, False)               AS is_milestone_stage,
      CASE WHEN stage_id IN (7,8,26) 
              THEN 'Screen'
            WHEN stage_id IN (23,47)  
              THEN 'Team Interview'
            WHEN stage_id IN (41, 57) 
              THEN 'Take Home Assessment'
            WHEN stage_id IN (22, 46) 
              THEN 'Hiring Manager Review'
            WHEN stage_id in (58,59)
              THEN 'Reference Check'
            ELSE application_stage_name END    AS stages_cleaned             
    FROM renamed 
)

SELECT *
FROM intermediate
