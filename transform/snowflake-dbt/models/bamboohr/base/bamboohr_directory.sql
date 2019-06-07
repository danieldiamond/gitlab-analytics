{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH source AS (

	SELECT *
    FROM {{ source('bamboohr', 'directory') }}
    ORDER BY uploaded_at
    DESC
    LIMIT 1

), renamed AS (

    SELECT
        d.value:displayName::STRING                             AS full_name,
        d.value:firstName::STRING                               AS first_name,
        d.value:gender::STRING                                  AS binary_gender,
        d.value:id::NUMBER                                      AS employee_id,
        d.value:jobTitle::STRING                                AS job_title,
        d.value:lastName::STRING                                AS last_name,
        nullif(d.value:preferredName::STRING,'null')::STRING    AS preferred_name
    FROM source
    , LATERAL FLATTEN(INPUT => parse_json(jsontext)) d

)

SELECT *
FROM renamed