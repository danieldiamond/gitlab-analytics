WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }}
    WHERE uploaded_at >= '2020-03-24'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(day,uploaded_at) ORDER BY uploaded_at) = 1

), intermediate AS (

    SELECT 
      NULLIF(d.value['employeeNumber'],'')::NUMBER                    AS employee_number,
      d.value['id']::NUMBER                                           AS employee_id,
      d.value['firstName']::VARCHAR                                   AS first_name,
      d.value['lastName']::VARCHAR                                    AS last_name,      
      (CASE WHEN d.value['hireDate']=''
            THEN NULL
           WHEN d.value['hireDate']= '0000-00-00'
            THEN NULL
           ELSE d.value['hireDate']::VARCHAR END)::DATE               AS hire_date,
      d.value['customLocality']::VARCHAR                              AS locality,
      DATE_TRUNC(day, uploaded_at)                                    AS updated_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['employees']), outer => true) d

)
 
SELECT 
  employee_number,
  employee_id,
  locality,
  updated_at
FROM intermediate
WHERE hire_date IS NOT NULL
  AND (LOWER(first_name) NOT LIKE '%greenhouse test%'
        AND locality IS NOT NULL
        AND LOWER(last_name) NOT LIKE '%test profile%'
        AND LOWER(last_name) != 'test-gitlab')
  AND employee_id NOT IN (42039, 42043)

---Note: the where clause is removing any test accounts and employee_id 42039 is also a test account


