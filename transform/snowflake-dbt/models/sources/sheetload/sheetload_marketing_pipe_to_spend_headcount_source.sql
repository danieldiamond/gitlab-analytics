WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'marketing_pipe_to_spend_headcount') }}

), renamed AS (


    SELECT
      date_month::DATE       AS date_month,
      pipe,
      headcount::REAL        AS headcount,
      salary_per_month::REAL AS salary_per_month
    FROM source

)

SELECT *
FROM renamed
