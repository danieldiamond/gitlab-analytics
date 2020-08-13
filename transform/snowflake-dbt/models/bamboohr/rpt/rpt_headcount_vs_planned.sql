{{ config({
    "schema": "analytics"
    })
}}

WITH headcount AS (
  
    SELECT 
      month_date, 
      CASE WHEN breakout_type = 'kpi_breakout' 
            THEN 'all_company_breakout'
           WHEN breakout_type = 'all_attributes_breakout' 
            THEN 'department_division_breakout'
           ELSE breakout_type END                                            AS breakout_type,
      IFF(breakout_type = 'kpi_breakout','all_company_breakout', department) AS department,
      IFF(breakout_type = 'kpi_breakout','all_company_breakout', division)   AS division,
      COALESCE(headcount_end,0)                                              AS headcount_actual,
      COALESCE(hire_count,0)                                                 AS hires_actual
    FROM {{ ref ('bamboohr_rpt_headcount_aggregation') }}
    WHERE breakout_type IN ('kpi_breakout','all_attributes_breakout','division_breakout')
      AND eeoc_field_name = 'no_eeoc'
  
), hire_plan AS (

    SElECT *,
      IFF(DATE_TRUNC(month, month_date) = DATE_TRUNC(month, DATEADD(month, -1, CURRENT_DATE())),1,0) AS last_month
    FROM {{ ref ('hire_replan_xf') }}
    WHERE breakout_type = 'department_division_breakout'

), division_mapping AS (

    SELECT 
      department,
      division
    FROM hire_plan
    WHERE last_month = 1
    GROUP BY 1,2
  
), final AS (

    SELECT 
      hire_plan.month_date,
      hire_plan.breakout_type,
      hire_plan.department,
      COALESCE(hire_plan.division, division_mapping.division)               AS division,
      hire_plan.planned_headcount,
      hire_plan.planned_hires,
      COALESCE(headcount.headcount_actual,0)                                AS headcount_actual,
      COALESCE(headcount.hires_actual,0)                                    AS hires_actual,
      IFF(hire_plan.planned_headcount = 0, NULL, 
        ROUND((headcount.headcount_actual/hire_plan.planned_headcount),4))  AS actual_headcount_vs_planned_headcount
    FROM hire_plan
    LEFT JOIN headcount
      ON headcount.breakout_type = hire_plan.breakout_type
      AND headcount.department = hire_plan.department
      AND headcount.division = hire_plan.division
      AND headcount.month_date = DATE_TRUNC(month, hire_plan.month_date)
    LEFT JOIN division_mapping
      ON hire_plan.department = division_mapping.department
      AND hire_plan.breakout_type = 'department_division_breakout'

       
)

SELECT *
FROM final
