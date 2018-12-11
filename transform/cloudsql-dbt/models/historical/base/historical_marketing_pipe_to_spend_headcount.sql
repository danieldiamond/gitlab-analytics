WITH source AS (

	SELECT *
	FROM historical.marketing_pipe_to_spend_headcount

), renamed AS (


	SELECT
       date_month::DATE,
	   pipe,
       headcount::REAL,
       salary_per_month::REAL
	FROM source

)

SELECT *
FROM renamed