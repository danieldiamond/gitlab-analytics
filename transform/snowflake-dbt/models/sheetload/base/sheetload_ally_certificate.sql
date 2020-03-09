WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'sheetload_ally_certificate') }}

)

SELECT *
FROM source
