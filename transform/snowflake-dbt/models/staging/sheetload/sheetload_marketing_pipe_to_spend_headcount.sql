WITH bamboohr AS (

    SELECT *
    FROM {{ ref('bamboohr_id_employee_number_mapping') }}
  
), source AS (

    SELECT *
    FROM {{ ref('sheetload_marketing_pipe_to_spend_headcount_source') }}

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
      ON renamed.employee_name = CONCAT(bamboohr.first_name, ' ', bamboohr.last_name)
    WHERE renamed.completed_date IS NOT NULL

)
SELECT *
FROM final