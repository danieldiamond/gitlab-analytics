WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','osat') }}
    
), renamed AS (

    SELECT
        TRY_TO_TIMESTAMP_NTZ(timestamp)::DATE                 AS completed_date,
        employee_name::VARCHAR                                AS employee_name,
        division::VARCHAR                                     AS division,
        NULLIF("SATISFACTION_SCORE",'')::INTEGER              AS satisfaction_score,
        NULLIF("RECOMMEND_TO_FRIEND",'')::INTEGER             AS recommend_to_friend,
        NULLIF(ONBOARDING_BUDDY_EXPERIENCE_SCORE,'')::INTEGER AS buddy_experience_score
    FROM source
 
), bamboohr AS (

    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping') }}
  
), final AS (

    SELECT
      completed_date,
      DATE_TRUNC(month, bamboohr.hire_date) AS hire_month,
      division
      satisfaction_score,
      recommend_to_friend,
      buddy_experience_score
    FROM renamed
    LEFT JOIN bamboohr
      ON renamed.employee_name = CONCAT(first_name,' ',last_name)
    WHERE completed_date IS NOT NULL

)
SELECT *
FROM final
