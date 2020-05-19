WITH source AS (

	SELECT *
	FROM {{ ref('ga360_session_hit_source') }}

)

SELECT *
FROM source
