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
 
), bamboohr AS (

    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping') }}
  
), final AS (

    SELECT
      renamed.completed_date,
      DATE_TRUNC(month, bamboohr.hire_date) AS hire_month,
      renamed.division,
      renamed.satisfaction_score,
      renamed.recommend_to_friend,
      renamed.buddy_experience_score
    FROM renamed
    LEFT JOIN bamboohr
      ON renamed.employee_name = CONCAT(bamboohr.first_name,' ',bamboohr.last_name)
    WHERE renamed.completed_date IS NOT NULL

)
SELECT *
FROM final
