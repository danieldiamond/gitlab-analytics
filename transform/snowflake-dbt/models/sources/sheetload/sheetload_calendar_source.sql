WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'calendar') }}

), renamed AS (

  SELECT
    NULLIF("Event_Title", '')::VARCHAR                                 AS event_title,
    NULLIF("Event_Location", '')::VARCHAR                              AS event_location,
    NULLIF("Event_Start", '')::VARCHAR::DATE                           AS event_start,
    NULLIF(NULLIF("Calculated_Duration"::VARCHAR, '#REF!'), '')::FLOAT AS calculated_duration,
    NULLIF("Date_Created", '')::VARCHAR::DATE                          AS date_created,
    NULLIF("Created_By", '')::VARCHAR                                  AS created_by
  FROM source

)

SELECT *
FROM renamed