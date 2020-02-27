
{% set repeated_metric_columns = 
      "SUM(headcount_start)                             AS headcount_start,
      SUM(headcount_end)                                AS headcount_end,
      (SUM(headcount_start) + SUM(headcount_end))/2     AS headcount_average,
      SUM(hire_count)                                   AS hire_count,
      SUM(separation_count)                             AS separation_count,
      SUM(voluntary_separation)                         AS voluntary_separation,
      SUM(involuntary_separation)                       AS involuntary_separation,

      SUM(headcount_start_leader)                       AS headcount_start_leader,
      SUM(headcount_end_leader)                         AS headcount_end_leader,
      (SUM(headcount_start_leader) 
        + SUM(headcount_end_leader))/2                  AS headcount_average_leader,
      SUM(hired_leaders)                                AS hired_leaders,
      SUM(separated_leaders)                            AS separated_leaders,

      SUM(headcount_start_manager)                      AS headcount_start_manager,
      SUM(headcount_end_manager)                        AS headcount_end_manager,
      (SUM(headcount_start_manager) 
        + SUM(headcount_end_leader))/2                  AS headcount_average_manager,
      SUM(hired_manager)                                AS hired_manager,
      SUM(separated_manager)                            AS separated_manager,

      SUM(headcount_start_contributor)                  AS headcount_start_contributor,
      SUM(headcount_end_contributor)                    AS headcount_end_individual_contributor,
      (SUM(headcount_start_contributor) 
        + SUM(headcount_end_contributor))/2             AS headcount_average_contributor,
      SUM(hired_contributor)                            AS hired_contributor,
      SUM(separated_contributor)                        AS separated_contributor
      " %}


with dates AS (

    SELECT
      date_actual                                 AS start_date,
      LAST_DAY(date_actual)                       AS end_date
    FROM {{ ref ('date_details') }}
    WHERE date_day <= LAST_DAY(current_date)
       AND day_of_month = 1
       AND date_actual >= '2013-07-01' -- min employment_status_date in bamboohr_employment_status model

), mapping AS (

    {{ dbt_utils.unpivot(relation=ref('bamboohr_id_employee_number_mapping'), cast_to='varchar', 
       exclude=['employee_number', 'employee_id','first_name', 'last_name', 'hire_date', 'termination_date', 'greenhouse_candidate_id']) }}

), mapping_enhanced AS (

    SELECT 
      employee_id,
      field_name                         AS eeoc_field_name, 
      COALESCE(value, 'Not Identified')  AS eeoc_value
    FROM mapping

    UNION ALL

    SELECT 
      DISTINCT employee_id,
      'no_eeoc'                         AS eeoc_field_name,
      'no eeoc'                         AS eeoc_value
    FROM mapping
 
), separation_reason AS(

    SELECT * 
    FROM {{ ref ('bamboohr_employment_status_xf') }}
    WHERE employment_status = 'Terminated'

), employees AS (

    SELECT *
    FROM {{ ref ('employee_directory_intermediate') }}
  
), job_info AS (

    SELECT  
      DISTINCT job_title,
      CASE WHEN job_title LIKE '%VP'          THEN 'leader'
           WHEN LEFT(job_title,5) = 'Chief'   THEN 'leader'
           WHEN job_title LIKE '%EVP'         THEN 'leader'
           WHEN job_title LIKE '%Manager,%'   THEN 'manager'
           ELSE 'individual_contributor'END   AS job_role
    FROM {{ ref ('bamboohr_job_info') }}

), intermediate AS (

    SELECT
      employees.date_actual,
      department,
      division,
      mapping_enhanced.eeoc_field_name,                                                       
      mapping_enhanced.eeoc_value,                                          
      IFF(dates.start_date = date_actual,1,0)                                   AS headcount_start,
      IFF(dates.end_date = date_actual,1,0)                                     AS headcount_end,
      IFF(is_hire_date = True, 1,0)                                             AS hire_count,
      IFF(is_termination_date = True,1,0)                                       AS separation_count,
      IFF(termination_type = 'Voluntary',1,0)                                   AS voluntary_separation,
      IFF(termination_type = 'Involuntary',1,0)                                 AS involuntary_separation,

      IFF(dates.start_date = date_actual 
          AND job_role = 'leader',1,0)                                          AS headcount_start_leader,
      IFF(dates.end_date = date_actual
          AND job_role = 'leader',1,0)                                          AS headcount_end_leader,
      IFF(is_hire_date = True 
          AND job_role = 'leader',1,0)                                          AS hired_leaders,
      IFF(is_termination_date = True
          AND job_role = 'leader',1,0)                                          AS separated_leaders,
      
      IFF(dates.start_date = date_actual 
          AND job_role = 'manager',1,0)                                          AS headcount_start_manager,
      IFF(dates.end_date = date_actual
          AND job_role = 'manager',1,0)                                          AS headcount_end_manager,
      IFF(is_hire_date = True 
          AND job_role = 'manager',1,0)                                          AS hired_manager,
      IFF(is_termination_date = True
          AND job_role = 'manager',1,0)                                          AS separated_manager,

       IFF(dates.start_date = date_actual 
          AND job_role = 'individual contributor',1,0)                           AS headcount_start_contributor,
      IFF(dates.end_date = date_actual
          AND job_role = 'individual contributor',1,0)                           AS headcount_end_contributor,
      IFF(is_hire_date = True 
          AND job_role = 'individual contributor',1,0)                           AS hired_contributor,
      IFF(is_termination_date = True
          AND job_role = 'individual contributor',1,0)                           AS separated_contributor                
    FROM dates
    LEFT JOIN employees
      ON DATE_TRUNC(month,dates.start_date) = DATE_TRUNC(month, employees.date_actual)
    LEFT JOIN mapping_enhanced
      ON employees.employee_id = mapping_enhanced.employee_id
    Left join job_info 
      ON job_info.job_title = employees.job_title
    LEFT JOIN separation_reason
      ON separation_reason.employee_id = employees.employee_id
      AND separation_reason.valid_from_date = employees.date_actual
   WHERE date_actual IS NOT NULL

), aggregated AS (


   SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'all_attributes_breakout'         AS breakout_type,
      department,
      division,
      eeoc_field_name,                                                       
      eeoc_value,    
     {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate 
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    {{ dbt_utils.group_by(n=6) }}  

    UNION ALL

    SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'eeoc_breakout'                   AS breakout_type, 
      null                              AS department,
      null                              AS division,
      eeoc_field_name,                                                       
      eeoc_value,  
      {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate 
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    {{ dbt_utils.group_by(n=6) }} 

    UNION ALL

    SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'division_breakout'               AS breakout_type, 
      null                              AS department,
      division,
      eeoc_field_name,                                                       
      eeoc_value,
      {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate 
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    WHERE department IS NOT NULL
    {{ dbt_utils.group_by(n=6) }} 

    UNION ALL

    SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'department_breakout'             AS breakout_type, 
      department,
      division,
      eeoc_field_name,                                                       
      eeoc_value,     
      {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    WHERE department IS NOT NULL
    {{ dbt_utils.group_by(n=6) }}  

) 

SELECT * 
FROM aggregated