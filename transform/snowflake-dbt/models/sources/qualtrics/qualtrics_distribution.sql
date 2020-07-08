WITH source AS (

    SELECT *
    FROM {{ source('qualtrics', 'distribution') }}

), intermediate AS (

    SELECT d.value as data_by_row
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d
    QUALIFY ROW_NUMBER() OVER(PARTITION BY data_by_row['id']::VARCHAR ORDER BY uploaded_at desc) = 1

), parsed AS (

    SELECT 
      data_by_row['recipients']['mailingListId']::VARCHAR   AS mailing_list_id,
      data_by_row['id']::VARCHAR                            AS distribution_id,
      data_by_row['surveyLink']['surveyId']::VARCHAR        AS survey_id,
      data_by_row['sendDate']::TIMESTAMP                    AS mailing_sent_at,
      data_by_row['stats']['blocked']::INTEGER              AS email_blocked_count,
      data_by_row['stats']['bounced']::INTEGER              AS email_bounced_count,
      data_by_row['stats']['complaints']::INTEGER           AS complaint_count,
      data_by_row['stats']['failed']::INTEGER               AS email_failed_count,
      data_by_row['stats']['finished']::INTEGER             AS survey_finished_count,
      data_by_row['stats']['opened']::INTEGER               AS email_opened_count,
      data_by_row['stats']['sent']::INTEGER                 AS email_sent_count,
      data_by_row['stats']['skipped']::INTEGER              AS email_skipped_count,
      data_by_row['stats']['started']::INTEGER              AS survey_started_count
    FROM intermediate

)
SELECT * 
FROM parsed
