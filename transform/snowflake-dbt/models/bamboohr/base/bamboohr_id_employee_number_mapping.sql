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

    SELECT nullif(d.value['employeeNumber'],'')::bigint                   AS employee_number,
          d.value['id']::bigint                                           AS employee_id,
          d.value['firstName']::varchar                                   AS first_name,
          d.value['lastName']::varchar                                    AS last_name,
          nullif(d.value['hireDate']::varchar,'0000-00-00')::date         AS hire_date,
          nullif(d.value['terminationDate']::varchar,'0000-00-00')::date  AS termination_date,
          d.value['customNationality']::varchar                           AS nationality,
          d.value['customRegion']::varchar                                AS region,
          d.value['ethnicity']::varchar                                   AS ethnicity
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['employees']), outer => true) d


)

SELECT *
FROM intermediate
WHERE hire_date IS NOT NULL
