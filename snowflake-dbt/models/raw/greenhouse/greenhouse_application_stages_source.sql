WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'application_stages') }}

), stage_dim AS (

    SELECT *
    FROM  {{ ref ('greenhouse_stages_source') }}

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
      is_milestone_stage,
      stage_name_modified
    FROM renamed
    LEFT JOIN stage_dim 
      ON renamed.stage_id = stage_dim.stage_id

)

SELECT *
FROM intermediate
