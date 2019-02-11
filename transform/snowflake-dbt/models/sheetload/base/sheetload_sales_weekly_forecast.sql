WITH source AS (

	SELECT *
	FROM {{ var("database") }}.sheetload.sales_weekly_forecast

), renamed AS (


	SELECT 	"Week"::date as week_of,
         	"Model"::varchar as model,
			NULLIF(trim("IACV"),'')::numeric as iacv
	FROM source

)

SELECT md5(week_of||model) as primary_key,
				*
FROM renamed
