WITH source AS (

	SELECT *
	FROM raw.historical.iacv_monthly_goals

), renamed AS (


	SELECT iacv_goal,
			date as date_day
	FROM source

)

SELECT *
FROM renamed
