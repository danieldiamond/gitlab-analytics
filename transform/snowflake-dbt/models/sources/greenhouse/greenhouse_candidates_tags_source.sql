WITH source as (

    SELECT *

      FROM {{ source('greenhouse', 'candidates_tags') }}

), renamed as (

    SELECT
			--keys
			  id::bigint 				    AS candidate_tag_id,
    		tag_id::bigint			   AS tag_id,
    		candidate_id::bigint	 AS candidate_id,

    		--info
    		created_at::timestamp  AS candidate_tag_created_at,
    		updated_at::timestamp  AS candidate_tag_updated_at


    FROM source

)

SELECT *
FROM renamed
