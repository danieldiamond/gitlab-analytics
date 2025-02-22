{{
    config({
      "schema": "sensitive"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'values_certificate') }}

), renamed AS (

    SELECT
      "Timestamp"::TIMESTAMP::DATE                                                                 AS completed_date,
      "Score"                                                                                      AS score,
      "First_&_Last_Name"                                                                          AS submitter_name,
      "Email_address_(GitLab_team_members,_please_use_your_GitLab_email_address)"::VARCHAR         AS submitter_email,
      "_UPDATED_AT"                                                                                AS last_updated_at
    FROM source

)

SELECT *
FROM renamed

