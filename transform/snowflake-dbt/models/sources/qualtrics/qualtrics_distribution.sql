WITH source AS (

    SELECT *
    FROM {{ source('qualtrics', 'distribution') }}

), intermediate AS (

    SELECT d.value as data_by_row
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), parsed AS (

    SELECT 
      data_by_row['recipients']['mailingListId']::VARCHAR   AS mailing_list_id,
      data_by_row['id']::VARCHAR                            AS distribution_id,
      data_by_row['surveyLink']['surveyId']::VARCHAR        AS survey_id,
      data_by_row['sendDate']::TIMESTAMP                    AS mailing_sent_at
    FROM intermediate

)
SELECT * 
FROM parsed
