{{config({
    "schema": "staging"
  })
}}

WITH source AS (

	SELECT *
		FROM {{ source('sheetload', 'marketing_pipe_to_spend_headcount') }}

), renamed AS (


	SELECT
       date_month::DATE as date_month,
	   pipe,
       headcount::REAL as headcount,
       salary_per_month::REAL as salary_per_month
	FROM source

)

SELECT *
FROM renamed
