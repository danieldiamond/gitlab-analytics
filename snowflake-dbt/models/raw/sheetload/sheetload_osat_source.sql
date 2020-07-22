WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','osat') }}
    
), renamed AS (

    SELECT
      TRY_TO_TIMESTAMP_NTZ("TIMESTAMP")::DATE               AS completed_date,
      "EMPLOYEE_NAME"::VARCHAR                              AS employee_name,
      "DIVISION"::VARCHAR                                   AS division,
      NULLIF("SATISFACTION_SCORE",'')::INTEGER              AS satisfaction_score,
      NULLIF("RECOMMEND_TO_FRIEND",'')::INTEGER             AS recommend_to_friend,
      NULLIF(ONBOARDING_BUDDY_EXPERIENCE_SCORE,'')::INTEGER AS buddy_experience_score
    FROM source
 
)

SELECT *
FROM renamed