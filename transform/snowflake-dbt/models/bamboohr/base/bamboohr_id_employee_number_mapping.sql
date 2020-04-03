{{ config({
    "materialized": "table"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT 
          NULLIF(d.value['employeeNumber'],'')::BIGINT                    AS employee_number,
          d.value['id']::BIGINT                                           AS employee_id,
          d.value['firstName']::VARCHAR                                   AS first_name,
          d.value['lastName']::VARCHAR                                    AS last_name,
          NULLIF(d.value['hireDate']::VARCHAR,'0000-00-00')::DATE         AS hire_date,
          NULLIF(d.value['terminationDate']::VARCHAR,'0000-00-00')::DATE  AS termination_date,
          d.value['customNationality']::VARCHAR                           AS nationality,
          d.value['customRegion']::VARCHAR                                AS region,
          d.value['ethnicity']::VARCHAR                                   AS ethnicity,
          d.value['gender']::VARCHAR                                      AS gender, 
          d.value['customCandidateID']::BIGINT                            AS greenhouse_candidate_id
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['employees']), outer => true) d


)


SELECT *
FROM intermediate
WHERE hire_date IS NOT NULL
    AND (LOWER(first_name) NOT LIKE '%greenhouse test%'
         AND LOWER(last_name) NOT LIKE '%test profile%'
         AND LOWER(last_name) != 'test-gitlab')
    AND employee_id != 42039
