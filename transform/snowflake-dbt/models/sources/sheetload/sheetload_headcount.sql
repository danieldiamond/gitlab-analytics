WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'headcount') }}

), renamed AS (


	SELECT 
      uniquekey::INTEGER                AS primary_key,
	  month::DATE                       AS month_of,
	  nullif(function, '')              AS function,
	  nullif(employee_cnt::INTEGER, '') AS employee_count
	FROM source

)

SELECT *
FROM renamed
