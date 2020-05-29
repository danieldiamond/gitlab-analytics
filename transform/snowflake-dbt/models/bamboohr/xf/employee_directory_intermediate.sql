{{ config({
    "materialized":"table",
    "schema": "sensitive"
    })
}}

WITH RECURSIVE employee_directory AS (

    SELECT
      employee_id,
      employee_number,
      first_name,
      last_name,
      work_email,
      hire_date,
      rehire_date,
      termination_date,
      hire_location_factor
    FROM {{ ref('employee_directory') }}

), date_details AS (

    SELECT *
    FROM {{ ref('date_details') }}

), department_info AS (

    SELECT employee_id,
          job_title,         
          department,
          division,
          reports_to,
          job_role,
          effective_date,
          effective_end_date
    FROM {{ ref('bamboohr_job_info') }}

), cost_center_bamboo AS (

    SELECT *
    FROM {{ ref ('bamboohr_job_role') }}

), location_factor AS (

    SELECT *
    FROM {{ ref('employee_location_factor_snapshots') }}

), employment_status AS (
    
    SELECT * 
     FROM {{ ref('bamboohr_employment_status_xf') }}

), employment_status_records_check AS (
    
    SELECT 
      employee_id,
      MIN(valid_from_date) AS employment_status_first_value
     FROM {{ ref('bamboohr_employment_status_xf') }}
     GROUP BY 1 

), cost_center_prior_to_bamboo AS (

    SELECT *
    FROM {{ ref('cost_center_division_department_mapping') }}

), enriched AS (

    SELECT
      date_actual,
      employee_directory.*,
      (employee_directory.first_name ||' '|| employee_directory.last_name)                                AS full_name,
      department_info.job_title,
      department_info.department,
      department_info.division,
      COALESCE(cost_center_bamboo.cost_center, 
               cost_center_prior_to_bamboo.cost_center)             AS cost_center,
      department_info.reports_to,
      department_info.job_role,
      location_factor.location_factor, 
      IFF(hire_date = date_actual OR 
          rehire_date = date_actual, True, False)                   AS is_hire_date,
      IFF(employment_status = 'Terminated', True, False)            AS is_termination_date,
      IFF(rehire_date = date_actual, True, False)                   AS is_rehire_date,
      IFF(hire_date< employment_status_first_value,
            'Active', employment_status)                            AS employment_status
    FROM date_details
    LEFT JOIN employee_directory
      ON hire_date::DATE <= date_actual
      AND COALESCE(termination_date::DATE, {{max_date_in_bamboo_analyses()}}) >= date_actual
    LEFT JOIN department_info
      ON employee_directory.employee_id = department_info.employee_id
      AND date_actual BETWEEN effective_date 
      AND COALESCE(effective_end_date::DATE, {{max_date_in_bamboo_analyses()}})
    LEFT JOIN location_factor
      ON employee_directory.employee_number::VARCHAR = location_factor.bamboo_employee_number::VARCHAR
      AND valid_from <= date_actual
      AND COALESCE(valid_to::DATE, {{max_date_in_bamboo_analyses()}}) >= date_actual
    LEFT JOIN employment_status
      ON employee_directory.employee_id = employment_status.employee_id 
      AND (date_details.date_actual = valid_from_date AND employment_status = 'Terminated' 
        OR date_details.date_actual BETWEEN employment_status.valid_from_date AND employment_status.valid_to_date )  
    LEFT JOIN employment_status_records_check 
      ON employee_directory.employee_id = employment_status_records_check.employee_id    
    LEFT JOIN cost_center_bamboo
      ON employee_directory.employee_id = cost_center_bamboo.employee_id
      AND date_details.date_actual BETWEEN cost_center_bamboo.effective_date 
                                       AND COALESCE(cost_center_bamboo.next_effective_date, {{max_date_in_bamboo_analyses()}})
    LEFT JOIN cost_center_prior_to_bamboo
      ON department_info.department = cost_center_prior_to_bamboo.department
      AND department_info.division = cost_center_prior_to_bamboo.division
      AND date_details.date_actual BETWEEN cost_center_prior_to_bamboo.effective_start_date 
                                       AND COALESCE(cost_center_prior_to_bamboo.effective_end_date, '2020-05-07')
    ---Starting 2020.05.08 we start capturing cost_center in bamboohr
    WHERE employee_directory.employee_id IS NOT NULL

), base_layers as (

    SELECT
      date_actual,
      reports_to,
      full_name,
      array_construct(reports_to, full_name) AS lineage
    FROM enriched
    WHERE NULLIF(reports_to, '') IS NOT NULL

), layers (date_actual, employee, manager, lineage, layers_count) AS (

    SELECT
      date_actual,
      full_name         AS employee,
      reports_to        AS manager,
      lineage           AS lineage,
      1                 AS layers_count
    FROM base_layers
    WHERE manager IS NOT NULL

    UNION ALL

    SELECT anchor.date_actual,
          iter.full_name    AS employee,
          iter.reports_to   AS manager,
          array_prepend(anchor.lineage, iter.reports_to) AS lineage,
          (layers_count+1)  AS layers_count
    FROM layers anchor
    JOIN base_layers iter
      ON anchor.date_actual = iter.date_actual
     AND iter.reports_to = anchor.employee


), calculated_layers AS (

    SELECT
      date_actual,
      employee,
      max(layers_count)     AS layers
    FROM layers
    GROUP BY 1, 2

)

SELECT
  enriched.*,
  COALESCE(calculated_layers.layers, 1) AS layers
FROM enriched
LEFT JOIN calculated_layers
  ON enriched.date_actual = calculated_layers.date_actual
  AND full_name = employee
  AND enriched.employment_status IS NOT NULL
WHERE employment_status IS NOT NULL
