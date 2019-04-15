{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH source AS (

	SELECT *
	FROM {{ var("database") }}.sheetload.headcount

), renamed AS (


	SELECT uniquekey::integer as primary_key,
			month::date as month_of,
			nullif(function, '') as function,
			nullif(employee_cnt::integer, '') as employee_count

	FROM source

)

SELECT *
FROM renamed
