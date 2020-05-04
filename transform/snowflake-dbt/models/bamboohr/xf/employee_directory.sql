WITH bamboohr_directory AS (

    SELECT *
    FROM {{ ref ('bamboohr_directory') }}

), department_info as (

    SELECT employee_id,
            last_value(job_title) RESPECT NULLS
                OVER ( PARTITION BY employee_id ORDER BY job_id ) AS last_job_title,
            last_value(reports_to) RESPECT NULLS
                OVER ( PARTITION BY employee_id ORDER BY job_id ) AS last_supervisor,
            last_value(department) RESPECT NULLS
                OVER ( PARTITION BY employee_id ORDER BY job_id ) AS last_department,
            last_value(division) RESPECT NULLS
                OVER ( PARTITION BY employee_id ORDER BY job_id ) AS last_division
    FROM {{ ref ('bamboohr_job_info') }}

), mapping as (

    SELECT *
    FROM {{ref('bamboohr_id_employee_number_mapping')}}

), location_factor as (

    SELECT distinct bamboo_employee_number,
            FIRST_VALUE(location_factor) OVER ( PARTITION BY bamboo_employee_number ORDER BY valid_from) AS hire_location_factor
    FROM {{ ref('employee_location_factor_snapshots') }}

), cost_center as (

    SELECT *
    FROM {{ref('cost_center_division_department_mapping')}}

), initial_hire AS (
    
    SELECT 
      employee_id,
      effective_date as hire_date
    FROM {{ref('bamboohr_employment_status')}}
    WHERE employment_status != 'Terminated'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date) = 1

), rehire AS (

    Select 
       employee_id,
       is_rehire,
       valid_from_date as rehire_date
    FROM {{ref('bamboohr_employment_status_xf')}}
    WHERE is_rehire='True'

), final AS (

    SELECT DISTINCT
      mapping.employee_id,
      mapping.employee_number,
      mapping.first_name,
      mapping.last_name,
      bamboohr_directory.work_email,
      iff(rehire.is_rehire = 'True', initial_hire.hire_date, mapping.hire_date) AS hire_date,
      rehire.rehire_date,
      mapping.termination_date,
      department_info.last_job_title,
      department_info.last_supervisor,
      department_info.last_department,
      department_info.last_division,
      cost_center.cost_center,
      location_factor.hire_location_factor
    FROM mapping
    LEFT JOIN bamboohr_directory
    ON bamboohr_directory.employee_id = mapping.employee_id
    LEFT JOIN department_info
    ON mapping.employee_id = department_info.employee_id
    LEFT JOIN cost_center
    ON trim(department_info.last_department)=trim(cost_center.department)
    AND trim(department_info.last_division)=trim(cost_center.division)
    LEFT JOIN location_factor
    ON location_factor.bamboo_employee_number = mapping.employee_number
    LEFT JOIN initial_hire 
    ON initial_hire.employee_id = mapping.employee_id
    LEFT JOIN rehire
    ON rehire.employee_id = mapping.employee_id
    WHERE mapping.hire_date < date_trunc('week', dateadd(week, 3, CURRENT_DATE))


)
SELECT * 
FROM final
WHERE employee_id != 42071
