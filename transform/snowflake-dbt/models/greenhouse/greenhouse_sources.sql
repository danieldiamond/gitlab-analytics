{{ config({
    "schema": "analytics"
    })
}}

WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'sources') }}

), renamed as (

	SELECT id 									AS source_id,

		--keys
	    organization_id,
	    name 										AS source_name,
	    type 										AS source_type,

	    created_at::timestamp 	AS created_at,
	    updated_at::timestamp 	AS updated_at

	FROM source
)

SELECT *
FROM renamed
