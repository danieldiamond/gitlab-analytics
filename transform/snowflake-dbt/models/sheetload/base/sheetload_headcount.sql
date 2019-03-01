{{ config(schema='analytics') }}

WITH source AS (

	SELECT *
	FROM {{ var("database") }}.sheetload.headcount

), renamed AS (


	SELECT uniquekey::integer as primary_key,
			month::date as month_of,
			function as function,
			employee_cnt::integer as employee_count

	FROM source

)

SELECT *
FROM renamed
