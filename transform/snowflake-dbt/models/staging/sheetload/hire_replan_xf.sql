WITH source AS (

    SELECT * 
    FROM {{ ref ('sheetload_hire_replan') }}
    
), original_FY21_plan AS (

    SELECT *
    FROM {{ ref ('hire_plan_xf') }}

), employee_directory AS (

    SELECT * 
    FROM {{ ref ('employee_directory_analysis') }}

), intermediate AS (
  
    SELECT 
      IFF("Departments"='Brand and Digital Design', 'Brand & Digital Design', 
            NULLIF("Departments", ''))::VARCHAR                 AS department,
      "4_30_2020"                                               AS "2020-04-30",
      "5_31_2020"                                               AS "2020-05-31",
      "6_30_2020"                                               AS "2020-06-30",
      "7_31_2020"                                               AS "2020-07-31",
      "8_31_2020"                                               AS "2020-08-31",
      "9_30_2020"                                               AS "2020-09-30",
      "10_31_2020"                                              AS "2020-10-31",
      "11_30_2020"                                              AS "2020-11-30",
      "12_31_2020"                                              AS "2020-12-31",
      "1_31_2021"                                               AS "2021-01-31",
      "2_28_2021"                                               AS "2021-02-28",
      "3_31_2021"                                               AS "2021-03-31",
      "4_30_2021"                                               AS "2021-04-30",
      "5_31_2021"                                               AS "2021-05-31",
      "6_30_2021"                                               AS "2021-06-30",
      "7_31_2021"                                               AS "2021-07-31",
      "8_31_2021"                                               AS "2021-08-31",
      "9_30_2021"                                               AS "2021-09-30",
      "10_31_2021"                                              AS "2021-10-31",
      "11_30_2021"                                              AS "2021-11-30",
      "12_31_2021"                                              AS "2021-12-31",
      "1_31_2022"                                               AS "2022-01-31"
    FROM source
   
), unpivoted AS (

    SELECT 
      department,
      month::DATE AS month_date,
      headcount
    FROM intermediate
        UNPIVOT (headcount for month IN (
        "2020-04-30",    
        "2020-05-31",
        "2020-06-30",
        "2020-07-31",
        "2020-08-31",
        "2020-09-30",
        "2020-10-31",
        "2020-11-30",
        "2020-12-31",
        "2021-01-31",
        "2021-02-28",
        "2021-03-31",
        "2021-04-30",
        "2021-05-31",
        "2021-06-30",
        "2021-07-31",
        "2021-08-31",
        "2021-09-30",
        "2021-10-31",
        "2021-11-30",
        "2021-12-31",
        "2022-01-31"))
  
), department_division_mapping AS (

    SELECT 
      unpivoted.month_date,
      unpivoted.department,
      employee_directory.division
    FROM unpivoted
    LEFT JOIN employee_directory
      ON unpivoted.month_date = employee_directory.date_actual
      AND unpivoted.department = employee_directory.department
    GROUP BY 1,2,3
  
), all_company AS (

    SELECT 
      unpivoted.month_date,
      'all_company_breakout'                                                AS breakout_type,
      'all_company_breakout'                                                AS department,
      'all_company_breakout'                                                AS division,
      SUM(headcount)                                                        AS planned_headcount,
      planned_headcount - lag(planned_headcount) OVER (ORDER BY month_date) AS planned_hires       
    FROM unpivoted
    GROUP BY 1,2,3
  
), division_level AS (

    SELECT 
      unpivoted.month_date,
      'division_breakout'                                                             AS breakout_type,
      'division_breakout'                                                             AS department,
      department_division_mapping.division,
      SUM(headcount)                                                                  AS planned_headcount,
      planned_headcount - lag(planned_headcount) 
        OVER (PARTITION BY department_division_mapping.division 
              ORDER BY unpivoted.month_date)                                          AS planned_hires     
    FROM unpivoted
    LEFT JOIN department_division_mapping 
      ON department_division_mapping.department = unpivoted.department
      AND department_division_mapping.month_date = unpivoted.month_date 
    GROUP BY 1,2,3,4

), department_level AS (

    SELECT 
      unpivoted.month_date,
      'department_division_breakout'                                                  AS breakout_type,
      unpivoted.department                                                            AS department,
      department_division_mapping.division,
      SUM(headcount)                                                                  AS planned_headcount,
      planned_headcount - lag(planned_headcount) 
        OVER (PARTITION BY department_division_mapping.division, unpivoted.department 
              ORDER BY unpivoted.month_date)                                          AS planned_hires        
    FROM unpivoted
    LEFT JOIN department_division_mapping 
      ON department_division_mapping.department = unpivoted.department
      AND department_division_mapping.month_date = unpivoted.month_date
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
  IFF(planned_hires<0, 0, COALESCE(planned_hires,0)) AS planned_hires
FROM unioned





