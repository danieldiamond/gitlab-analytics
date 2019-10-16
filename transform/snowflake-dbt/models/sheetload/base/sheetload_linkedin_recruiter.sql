WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','linkedin_recruiter') }}
    
), renamed AS (

    SELECT 
      seat_holder::VARCHAR                                 AS sourcer,
      ZEROIFNULL(NULLIF("MESSAGES_SENT",''))::INTEGER      AS messages_sent,
      ZEROIFNULL(NULLIF("RESPONSES_RECEIVED",''))::INTEGER AS responses_received,
      ZEROIFNULL(NULLIF("ACCEPTANCES",''))::INTEGER        AS acceptances,
      ZEROIFNULL(NULLIF("DECLINES",''))::INTEGER           AS declines,
      ZEROIFNULL(NULLIF("NO_RESPONSE",''))::INTEGER        AS no_response,
      ZEROIFNULL(NULLIF("RESPONSES_RATE",''))::INTEGER     AS responses_rate,
      ZEROIFNULL(NULLIF("ACCEPT_RATE",''))::INTEGER        AS accept_rate,
      month::DATE                                          AS data_downloaded_month
    FROM source
      
) 

SELECT *
FROM renamed
WHERE month IS NOT NULL 
  AND sourcer IS NOT NULL