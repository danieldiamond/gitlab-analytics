{{
    config({
      "schema": "sensitive"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'compensation_certificate') }}

), renamed as (

    SELECT
      "Timestamp"::TIMESTAMP::DATE                                                                 AS completed_date,
      "Score"                                                                                      AS score,
      "First_&_Last_Name"                                                                          AS submitter_name,
      "Email_Address"::STRING                                                                      AS submitter_email,
      "_UPDATED_AT"                                                                                AS last_updated_at
    FROM source

)

SELECT *
FROM renamed
