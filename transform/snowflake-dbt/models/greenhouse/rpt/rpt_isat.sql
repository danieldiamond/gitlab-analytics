{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

WITH interview_results AS (

   SELECT 
     candidate_survey_id, 
     organization_id, 
     department_id, 
     department_name, 
     DATE_TRUNC('month', candidate_survey_submitted_at)::DATE as submitted_at,
     candidate_survey_question_1,
    CASE
      WHEN candidate_survey_question_1 = 'Strongly Disagree' THEN 1
      WHEN candidate_survey_question_1 = 'Disagree' THEN 2
      WHEN candidate_survey_question_1 = 'Neutral' THEN 3
      WHEN candidate_survey_question_1 = 'Agree' THEN 4
      WHEN candidate_survey_question_1 = 'Strongly Agree' THEN 5
     ELSE NULL END AS isat_score
   FROM {{ ref('greenhouse_candidate_surveys') }}
   WHERE isat_score IS NOT NULL
   GROUP BY 1,2,3,4,5,6,7

 )

SELECT *
FROM final
