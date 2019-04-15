with retention_reasons_for_expansion AS (

	SELECT *
	FROM {{ref('retention_reasons_for_expansion')}}

)

SELECT *
FROM retention_reasons_for_expansion
WHERE expansion_type = 'Unknown'