WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'application_stages') }}

), renamed as (

	SELECT
    		--keys
    		application_id::bigint		AS application_id,
    		stage_id::bigint			    AS stage_id,

    		--info
    		entered_on::timestamp 		AS application_entered_on,
    		exited_on::timestamp 		  AS application_exited_on,
    		stage_name::varchar 		  AS application_stage_name

	FROM source

), intermediate AS (

    SELECT 
      renamed.*,
      IFF(stage_id in (7 --screen
                   ,8 -- Screen
                   ,26 --screening
                   ,52 --assessment
                   ,33 --face to face
                   ,61 --offer
                   ),True, False)               AS is_milestone_stage,
        CASE WHEN stage_id IN (7,8,26) 
               THEN 'Sreen'
             WHEN stage_id IN (25,47)  
               THEN 'Team Interview'
             WHEN stage_id IN (41, 57) 
               THEN 'Take Home Assessment'
             WHEN stage_id IN (24, 46) 
               THEN 'Hiring Manager Review'
             ELSE NULL END                      AS stages_cleaned             
    FROM renamed 
)

SELECT *
FROM intermediate
