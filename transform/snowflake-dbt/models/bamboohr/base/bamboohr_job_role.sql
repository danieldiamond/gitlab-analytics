WITH source AS (
    
    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }} 
  
), intermediate AS (

    SELECT 
          NULLIF(d.value['employeeNumber'],'')::BIGINT                    AS employee_number,
          d.value['id']::BIGINT                                           AS employee_id,
          d.value['firstName']::VARCHAR                                   AS first_name,
          d.value['lastName']::VARCHAR                                    AS last_name,
          d.value['customRole']::VARCHAR                                  AS job_role,
          d.value['customJobGrade']::VARCHAR                              AS job_grade,
          uploaded_at::DATE                                               AS effective_date
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['employees']), OUTER => true) d
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, job_role ORDER BY effective_date)=1  

), final AS (

    SELECT 
      intermediate.*,
      LEAD(DATEADD(day,-1,effective_date)) OVER (PARTITION BY employee_number ORDER BY effective_date)  AS next_effective_date
    FROM intermediate 
    
) 

SELECT * 
FROM final
WHERE effective_date>= '2020-02-27'  --1st day we started capturing 
