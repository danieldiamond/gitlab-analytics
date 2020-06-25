WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'headcount') }}

), renamed AS (


	SELECT uniquekey::integer as primary_key,
			month::date as month_of,
			nullif(function, '') as function,
			nullif(employee_cnt::integer, '') as employee_count

	FROM source

)

SELECT *
FROM renamed
