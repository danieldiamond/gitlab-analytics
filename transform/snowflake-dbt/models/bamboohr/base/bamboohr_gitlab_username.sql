{{ config({
    "materialized": "table"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }}
    WHERE DATE_TRUNC(day,uploaded_at) >= '2020-06-01'
    --1st date we started capturing GitLab_username
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(day,uploaded_at) ORDER BY uploaded_at DESC) = 1

), parsed AS (

    SELECT 
      NULLIF(d.value['employeeNumber'],'')::BIGINT                    AS employee_number,
      d.value['id']::BIGINT                                           AS employee_id,
      d.value['firstName']::VARCHAR                                   AS first_name,
      d.value['lastName']::VARCHAR                                    AS last_name,      
      NULLIF(d.value['hireDate']::VARCHAR,'0000-00-00')::DATE         AS hire_date,
      NULLIF(d.value['terminationDate']::VARCHAR,'0000-00-00')::DATE  AS termination_date,
      d.value['customGitLabUsername']::VARCHAR                        AS gitlab_username,
      DATE_TRUNC(day, uploaded_at)                                    AS updated_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['employees']), outer => true) d

), intermediate AS (
 
    SELECT 
    employee_number,
    employee_id,
    termination_date,
    gitlab_username,
    updated_at
    FROM parsed
    WHERE hire_date IS NOT NULL
    AND (LOWER(first_name) NOT LIKE '%greenhouse test%'
            AND LOWER(last_name) NOT LIKE '%test profile%'
            AND LOWER(last_name) != 'test-gitlab')
    AND employee_id != 42039
---Note: the where clause is removing any test accounts and employee_id 42039 is also a test account
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, gitlabusername ORDER BY updated_at) = 1
  
  
) ,final AS (

    SELECT
      employee_id,
      gitlab_username,
      updated_at,
      COALESCE(LEAD(updated_at) OVER (PARTITION BY employee_id ORDER BY updated_at, gitlabusername), CURRENT_DATE())     AS valid_to_date
    FROM intermediate
    WHERE updated_at<=COALESCE(termination_date,CURRENT_DATE())
      AND gitlabusername IS NOT NULL

)

SELECt *
FROM final 



