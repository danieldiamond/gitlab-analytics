WITH source AS (

	SELECT *
	FROM {{ ref('ga360_session_source') }}

)

SELECT *
FROM source
