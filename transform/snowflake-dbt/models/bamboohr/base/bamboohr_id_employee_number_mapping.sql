{{ config(materialized='table') }}

with source as (

    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate as (

      SELECT nullif(d.value['employeeNumber'],'')::bigint AS employee_number,
            d.value['firstName']::varchar                 AS first_name,
            d.value['id']::bigint                         AS employee_id,
            d.value['lastName']::varchar                  AS last_name
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext['employees']), outer => true) d



)

SELECT *
FROM intermediate
