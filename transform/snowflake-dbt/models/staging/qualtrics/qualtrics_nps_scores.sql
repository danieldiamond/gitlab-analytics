WITH answers AS (

    SELECT *
    FROM {{ ref('qualtrics_nps_answers') }}
    WHERE question_id IN ('QID172787673', 'QID172787675_TEXT')

)

SELECT DISTINCT
  response_id,
  distribution_channel,
  has_finished_survey,
  user_language,
  MAX(response_recorded_at) OVER (PARTITION BY response_id)                AS max_response_recorded_at,
  IFF(question_id = 'QID172787673', question_response, NULL)::INTEGER      AS nps_score,
  IFF(question_id = 'QID172787675_TEXT', question_response, NULL)::VARCHAR AS nps_reason
FROM answers
ORDER BY response_recorded_at
