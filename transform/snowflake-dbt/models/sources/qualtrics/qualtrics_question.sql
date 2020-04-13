WITH source AS (

    SELECT *
    FROM {{ source('qualtrics', 'questions') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), questions AS (

  SELECT d.value AS data_by_row
  FROM source,
  LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d
  
), parsed AS (

  SELECT 
    data_by_row['survey_id']::VARCHAR               AS survey_id,
    data_by_row['QuestionID']::VARCHAR              AS question_id,
    data_by_row['QuestionDescription']::VARCHAR     AS question_description,
    data_by_row['Choices']::ARRAY                   AS answer_choices
  FROM questions

)
SELECT * 
FROM parsed