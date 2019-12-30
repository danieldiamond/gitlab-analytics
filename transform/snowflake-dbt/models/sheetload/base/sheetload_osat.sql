WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','osat') }}
    
), renamed AS (

    SELECT
        TRY_TO_TIMESTAMP_NTZ(timestamp)::DATE                 AS completed_date,
        'Anonymous'                                           AS employee_name, 
        ZEROIFNULL(NULLIF("SATISFACTION_SCORE",''))::INTEGER  AS satisfaction_score,
        ZEROIFNULL(NULLIF("RECOMMEND_TO_FRIEND",''))::INTEGER AS recommend_to_friend
    FROM source 
      
) 

SELECT *
FROM renamed
WHERE completed_date is not NULL
