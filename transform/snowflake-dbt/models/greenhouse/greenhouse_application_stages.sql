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

)

SELECT *
FROM renamed
