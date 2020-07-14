WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'headcount') }}

), renamed AS (


    SELECT 
      uniquekey::INTEGER                AS primary_key,
      month::DATE                       AS month_of,
      NULLIF(function, '')              AS function,
      NULLIF(employee_cnt::INTEGER, '') AS employee_count
    FROM source

)

SELECT *
FROM renamed
