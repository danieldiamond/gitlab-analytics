WITH source AS (
    
    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }} 
  
), intermediate AS (

    SELECT 
      NULLIF(d.value['employeeNumber'],'')::BIGINT                    AS employee_number,
      d.value['id']::BIGINT                                           AS employee_id,
      d.value['firstName']::VARCHAR                                   AS first_name,
      d.value['lastName']::VARCHAR                                    AS last_name,
      NULLIF(d.value['hireDate']::VARCHAR,'0000-00-00')::DATE         AS hire_date,
      d.value['customRole']::VARCHAR                                  AS job_role,
      d.value['customJobGrade']::VARCHAR                              AS job_grade,
      d.value['customCostCenter']::VARCHAR                            AS cost_center,
      d.value['customJobTitleSpeciality']::VARCHAR                    AS jobtitle_speciality,
      uploaded_at::DATETIME                                           AS effective_date
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['employees']), OUTER => true) d
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, job_role, job_grade, cost_center, jobtitle_speciality 
            ORDER BY DATE_TRUNC(day,effective_date) ASC, DATE_TRUNC(hour, effective_date) DESC)=1  


), final AS (

    SELECT 
      employee_number,
      employee_id,
      job_role,
      job_grade,
      cost_center,
      jobtitle_speciality,
      DATE_TRUNC(day, effective_date)                                                    AS effective_date,
      LEAD(DATEADD(day,-1,DATE_TRUNC(day, intermediate.effective_date))) OVER 
        (PARTITION BY employee_number ORDER BY intermediate.effective_date)              AS next_effective_date
    FROM intermediate 
    WHERE effective_date>= '2020-02-27'  --1st day we started capturing job role
      AND hire_date IS NOT NULL
      AND (LOWER(first_name) NOT LIKE '%greenhouse test%'
      AND LOWER(last_name) NOT LIKE '%test profile%'
      AND LOWER(last_name) != 'test-gitlab')
      AND employee_id != 42039

) 

SELECT * 
FROM final
