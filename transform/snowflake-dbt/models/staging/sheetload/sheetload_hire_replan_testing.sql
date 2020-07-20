WITH hire_replan AS (

   {{ dbt_utils.unpivot(
      relation=ref('sheetload_hire_replan'),
      cast_to='integer',
      exclude=['department'],
      field_name = 'month_date',
      value_name = 'planned_headcount'
      ) }}

), original_FY21_plan AS (

    SELECT *
    FROM {{ ref ('hire_plan_xf') }}

), employee_directory AS (

    SELECT * 
    FROM {{ ref ('employee_directory_analysis') }}
  
), department_division_mapping AS (

    SELECT 
      hire_replan.month_date,
      hire_replan.department,
      employee_directory.division
    FROM hire_replan
    LEFT JOIN employee_directory
      ON hire_replan.month_date = employee_directory.date_actual
      AND hire_replan.department = employee_directory.department
    GROUP BY 1,2,3
  
), all_company AS (

    SELECT 
      hire_replan.month_date,
      'all_company_breakout'                                                AS breakout_type,
      'all_company_breakout'                                                AS department,
      'all_company_breakout'                                                AS division,
      SUM(planned_headcount)                                                AS planned_headcount
      {# SUM(planned_headcount) - lag(SUM(planned_headcount)) OVER (ORDER BY month_date) AS planned_hires         #}
    FROM hire_replan
    GROUP BY 1,2,3,4
  
 ), division_level AS (

    SELECT 
      hire_replan.month_date,
      'division_breakout'                                                             AS breakout_type,
      'division_breakout'                                                             AS department,
      department_division_mapping.division,
      SUM(planned_headcount)                                                          AS planned_headcount
            {# SUM(planned_headcount) - lag(SUM(planned_headcount)) OVER (ORDER BY month_date) AS planned_hires        

      planned_headcount - lag(planned_headcount) 
        OVER (PARTITION BY department_division_mapping.division 
              ORDER BY hire_replan.month_date)                                        AS planned_hires      #}
    FROM hire_replan
    LEFT JOIN department_division_mapping 
      ON department_division_mapping.department = hire_replan.department
      AND department_division_mapping.month_date = hire_replan.month_date 
    GROUP BY 1,2,3,4

), department_level AS (

    SELECT 
      hire_replan.month_date,
      'department_division_breakout'                                                    AS breakout_type,
      hire_replan.department                                                            AS department,
      department_division_mapping.division,
      SUM(planned_headcount)                                                            AS planned_headcount
      {# planned_headcount - lag(planned_headcount) 
        OVER (PARTITION BY department_division_mapping.division, hire_replan.department 
              ORDER BY hire_replan.month_date)                                          AS planned_hires         #}
    FROM hire_replan
    LEFT JOIN department_division_mapping 
      ON department_division_mapping.department = hire_replan.department
      AND department_division_mapping.month_date = hire_replan.month_date
    GROUP BY 1,2,3,4

), unioned AS (

    SELECT *
    FROM original_FY21_plan
    ----this plan captures the monthly plan prior to 2020.05, whereas the replan captures months post 2020.05

    UNION ALL

    SELECT *
    FROM all_company 
    WHERE month_date >='2020-06-01'

    UNION All

    SELECT * 
    FROM division_level
    WHERE month_date >='2020-06-01'


    UNION ALL 

    SELECT *
    FROM department_level
    WHERE month_date >='2020-06-01'

)

SELECT
  month_date,
  breakout_type,
  department,
  division,
  planned_headcount,
  planned_headcount - lag(planned_headcount) 
        OVER (PARTITION BY breakout_type, department, division
              ORDER BY hire_replan.month_date)       AS planned_hires        
  {# IFF(planned_hires<0, 0, COALESCE(planned_hires,0)) AS planned_hires #}
FROM unioned




