WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'company_call_attendance') }}

), renamed AS (

    SELECT
      "Date"::DATE         AS call_date,
      "Topic"::VARCHAR     AS meeting_topic,
      DAYNAME(call_date)   AS day_of_the_week,
      "Start_Time"         AS call_time,
      "Participants"       AS count_of_participants
    FROM source

)

SELECT *
FROM renamed
