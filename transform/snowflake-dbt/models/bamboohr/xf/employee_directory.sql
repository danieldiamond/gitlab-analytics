WITH bamboohr_directory AS (

    SELECT *
    FROM {{ ref ('bamboohr_directory') }}

), department_info as (

    SELECT employee_id,
            last_value(job_title) RESPECT NULLS
                OVER ( PARTITION BY employee_id ORDER BY effective_date ) AS last_job_title,
            last_value(reports_to) RESPECT NULLS
                OVER ( PARTITION BY employee_id ORDER BY effective_date ) AS last_supervisor,
            last_value(department) RESPECT NULLS
                OVER ( PARTITION BY employee_id ORDER BY effective_date ) AS last_department,
            last_value(division) RESPECT NULLS
                OVER ( PARTITION BY employee_id ORDER BY effective_date ) AS last_division
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
)

SELECT distinct
        mapping.employee_id,
        mapping.employee_number,
        mapping.first_name,
        mapping.last_name,
        bamboohr_directory.work_email,
        mapping.hire_date,
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
WHERE hire_date < date_trunc('week', dateadd(week, 3, CURRENT_DATE))
AND mapping.employee_id NOT IN (
                              '41683', --https://gitlab.com/gitlab-data/analytics/issues/2749
                              '41692', --https://gitlab.com/gitlab-data/analytics/issues/2749
                              '41693', --https://gitlab.com/gitlab-data/analytics/issues/2882
                              '41835' --https://gitlab.com/gitlab-data/analytics/issues/3219
                            )

ORDER BY hire_date DESC
