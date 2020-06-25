WITH source AS (

    SELECT 
      month_year            AS month_date,
      department, 
      plan                  AS headcount
    FROM {{ ref ('sheetload_hire_plan') }}
    WHERE month_year <='2020-04-30'

), department_division_mapping AS (

    SELECT 
      source.month_date,
      source.department,
      employee_directory.division
    FROM source
    LEFT JOIN "ANALYTICS"."ANALYTICS"."EMPLOYEE_DIRECTORY_ANALYSIS" employee_directory
      ON DATE_TRUNC(month,source.month_date) = DATE_TRUNC(month,employee_directory.date_actual)
      AND CASE WHEN employee_directory.department LIKE '%People%' 
                THEN 'People' 
               WHEN employee_directory.department ='Brand & Digital Design'
                 THEN 'Brand and Digital Design' 
               WHEN employee_directory.department = 'Community Relations' AND date_actual <'2020-02-29'
                 THEN 'Outreach' ELSE employee_directory.department END = source.department
    GROUP BY 1,2,3
      
), all_company AS (

    SELECT 
      source.month_date,
      'all_company_breakout'                                                AS breakout_type,
      'all_company_breakout'                                                AS department,
      'all_company_breakout'                                                AS division,
      SUM(headcount)                                                        AS planned_headcount,
      planned_headcount - lag(planned_headcount) OVER (ORDER BY month_date) AS planned_hires       
    FROM source
    GROUP BY 1,2,3
  
), division_level AS (

    SELECT 
      source.month_date,
      'division_breakout'                                                   AS breakout_type,
      'division_breakout'                                                   AS department,
      department_division_mapping.division,
      SUM(headcount)                                                        AS planned_headcount,
      planned_headcount - lag(planned_headcount) 
        OVER (ORDER BY source.month_date)                                   AS planned_hires       
    FROM source
    LEFT JOIN department_division_mapping 
      ON department_division_mapping.department = source.department
      AND department_division_mapping.month_date = source.month_date
    GROUP BY 1,2,3,4

), department_level AS (

    SELECT 
      source.month_date,
      'department_division_breakout'                                     AS breakout_type,
      source.department                                                  AS department,
      department_division_mapping.division,
      SUM(headcount)                                                     AS planned_headcount,
      planned_headcount - lag(planned_headcount) 
        OVER (ORDER BY source.month_date)                                AS planned_hires       
    FROM source
    LEFT JOIN department_division_mapping 
      ON department_division_mapping.department = source.department
      AND department_division_mapping.month_date = source.month_date
    GROUP BY 1,2,3,4

)

SELECT *
FROM all_company 

UNION All

SELECT * 
FROM division_level

UNION ALL 

SELECT *
FROM department_level