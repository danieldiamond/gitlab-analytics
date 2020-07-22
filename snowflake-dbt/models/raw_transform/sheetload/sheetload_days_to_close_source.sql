WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'days_to_close') }}

), renamed AS (

    SELECT 
      close_month::DATE          AS close_month,
      days_to_close::INT         AS days_to_close,
      days_to_close_target::INT  AS days_to_close_target
    FROM source

)

SELECT * 
FROM renamed
