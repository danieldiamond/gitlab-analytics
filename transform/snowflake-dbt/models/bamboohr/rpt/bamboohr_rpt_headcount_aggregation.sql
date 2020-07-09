{{ config({
    "schema": "analytics"
    })
}}

 
{% set partition_statement = "OVER (PARTITION BY base.breakout_type, base.department, base.division, base.eeoc_field_name, base.eeoc_value
                              ORDER BY base.month_date DESC ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING)
                              " %}


{% set ratio_to_report_partition_statement = "OVER (PARTITION BY base.breakout_type, base.department, base.division, base.eeoc_field_name
                                              ORDER BY base.month_date)
                              " %}

WITH source AS (

    SELECT *
    FROM {{ ref ('bamboohr_headcount_intermediate') }}

), base AS (

    SELECT DISTINCT 
      month_date,
      breakout_type, 
      department,
      division,
      eeoc_field_name,                                                       
      eeoc_value
    FROM source

), intermediate AS (

   SELECT
      base.month_date,
      IFF(base.breakout_type = 'eeoc_breakout' and base.eeoc_field_name = 'no_eeoc', 'kpi_breakout',base.breakout_type) AS breakout_type, 
      base.department,
      base.division,
      base.eeoc_field_name,
      base.eeoc_value,
      headcount_start,
      headcount_end,
      headcount_average,
      hire_count,
      separation_count,
      voluntary_separation,
      involuntary_separation,
      AVG(COALESCE(headcount_average, 0)) {{partition_statement}}                   AS rolling_12_month_headcount,
      SUM(COALESCE(separation_count,0)) {{partition_statement}}                     AS rolling_12_month_separations,
      SUM(COALESCE(voluntary_separation,0)) {{partition_statement}}                 AS rolling_12_month_voluntary_separations,
      SUM(COALESCE(involuntary_separation,0)) {{partition_statement}}               AS rolling_12_month_involuntary_separations,
      IFF(rolling_12_month_headcount< rolling_12_month_separations, NULL,
        1 - (rolling_12_month_separations/NULLIF(rolling_12_month_headcount,0)))    AS retention,

      headcount_end_leader,
      headcount_average_leader,
      hired_leaders,
      separated_leaders,
      AVG(COALESCE(headcount_average_leader, 0)) {{partition_statement}}             AS rolling_12_month_headcount_leader,
      SUM(COALESCE(separated_leaders,0)) {{partition_statement}}                     AS rolling_12_month_separations_leader,
      IFF(rolling_12_month_headcount_leader< rolling_12_month_separations_leader, NULL,
        1 - (rolling_12_month_separations_leader/NULLIF(rolling_12_month_headcount_leader,0)))    AS retention_leader,

      headcount_end_manager,
      headcount_average_manager,
      hired_manager,
      separated_manager,
      AVG(COALESCE(headcount_average_manager, 0)) {{partition_statement}}             AS rolling_12_month_headcount_manager,
      SUM(COALESCE(separated_manager,0)) {{partition_statement}}                      AS rolling_12_month_separations_manager,
      IFF(rolling_12_month_headcount_manager< rolling_12_month_separations_manager, NULL,
        1 - (rolling_12_month_separations_manager/NULLIF(rolling_12_month_headcount_manager,0)))    AS retention_manager,

      headcount_end_individual_contributor,
      headcount_average_contributor,
      hired_contributor,
      separated_contributor,
      
      MIN(headcount_end_individual_contributor)
        {{ratio_to_report_partition_statement}}                                     AS min_headcount_end_contributor, 
      MIN(headcount_average)  {{ratio_to_report_partition_statement}}               AS min_headcount_average,
      MIN(hire_count) {{ratio_to_report_partition_statement}}                       AS min_hire_count,
      MIN(headcount_average_leader) {{ratio_to_report_partition_statement}}         AS min_headcount_leader,
      MIN(headcount_average_manager) {{ratio_to_report_partition_statement}}        AS min_headcount_manager,
      MIN(headcount_average_contributor) {{ratio_to_report_partition_statement}}    AS min_headcount_contributor,


      RATIO_TO_REPORT(headcount_average) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount,
      RATIO_TO_REPORT(hire_count) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_hires,
      RATIO_TO_REPORT(headcount_average_leader) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount_leaders,
      RATIO_TO_REPORT(headcount_average_manager) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount_manager,     
      RATIO_TO_REPORT(headcount_average_contributor) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount_contributor
      
    FROM base
    LEFT JOIN source  
      ON base.month_date = source.month_date
      AND base.breakout_type = source.breakout_type
      AND base.department = source.department
      AND base.division = source.division
      AND base.eeoc_field_name = source.eeoc_field_name
      AND base.eeoc_value = source.eeoc_value
    WHERE base.month_date < DATE_TRUNC('month', CURRENT_DATE)   

 ), final AS (
     
    SELECT   
      month_date,
      breakout_type, 
      department,
      division,
      eeoc_field_name,
      eeoc_value,
      IFF(headcount_start <4 AND eeoc_field_name != 'no_eeoc', 
        NULL,headcount_start)                                               AS headcount_start,
      IFF(headcount_end <4 AND eeoc_field_name != 'no_eeoc',
        NULL, headcount_end)                                                AS headcount_end,
      IFF(headcount_average <4 AND eeoc_field_name != 'no_eeoc',  
        NULL, headcount_average)                                            AS headcount_average,
      IFF(hire_count <4 AND eeoc_field_name != 'no_eeoc', 
        NULL, hire_count)                                                   AS hire_count,
      IFF(separation_count <4 AND eeoc_field_name != 'no_eeoc', 
        NULL, separation_count)                                             AS separation_count,
      
      rolling_12_month_headcount,
      rolling_12_month_separations,
      rolling_12_month_voluntary_separations,
      rolling_12_month_involuntary_separations,
      IFF(rolling_12_month_headcount< rolling_12_month_voluntary_separations, NULL,
        (rolling_12_month_voluntary_separations/NULLIF(rolling_12_month_headcount,0)))    AS voluntary_separation_rate,
      IFF(rolling_12_month_headcount< rolling_12_month_involuntary_separations, NULL,
        (rolling_12_month_involuntary_separations/NULLIF(rolling_12_month_headcount,0)))  AS involuntary_separation_rate,
      retention,

      IFF(headcount_end_leader < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_end_leader)                                         AS headcount_end_leader, 
      IFF(headcount_average_leader < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL,headcount_average_leader)                                      AS headcount_leader_average,
      IFF(hired_leaders < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, hired_leaders)                                                AS hired_leaders,
      IFF(separated_leaders < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, separated_leaders)                                            AS separated_leaders,
      rolling_12_month_headcount_leader,
      rolling_12_month_separations_leader,
      retention_leader,


      IFF(headcount_end_manager < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_end_manager)                                        AS headcount_end_manager,            
      IFF(headcount_average_manager < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_average_manager)                                    AS headcount_manager_average,
      IFF(hired_manager < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, hired_manager)                                                AS hired_manager,
      IFF(separated_manager < 2 AND eeoc_field_name != 'no_eeoc',
        NULL, separated_manager)                                            AS separated_manager,
      rolling_12_month_headcount_manager,
      rolling_12_month_separations_manager,
      retention_manager,
 
      IFF(headcount_end_individual_contributor < 4 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_end_individual_contributor)                         AS headcount_end_contributor,
      IFF(headcount_average_contributor < 4 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_average_contributor)                                AS headcount_contributor,
      IFF(hired_contributor < 4 AND eeoc_field_name != 'no_eeoc', 
        NULL, hired_contributor)                                            AS hired_contributor,
      IFF(separated_contributor < 4 AND eeoc_field_name != 'no_eeoc',
        NULL, separated_contributor)                                        AS separated_contributor,

      IFF(min_headcount_average <2 AND eeoc_field_name != 'no_eeoc', 
        NULL, percent_of_headcount)                                         AS percent_of_headcount,
      IFF(min_hire_count <2 AND eeoc_field_name != 'no_eeoc',
        NULL, percent_of_hires)                                             AS percent_of_hires,
      IFF(min_headcount_leader <2 AND eeoc_field_name != 'no_eeoc', 
        NULL, percent_of_headcount_leaders)                                 AS percent_of_headcount_leaders,
      IFF(min_headcount_manager <2 AND eeoc_field_name != 'no_eeoc', 
        NULL, percent_of_headcount_manager)                                 AS percent_of_headcount_manager,
      IFF(min_headcount_contributor <2 AND eeoc_field_name != 'no_eeoc', 
        NULL, percent_of_headcount_leaders)                                 AS percent_of_headcount_contributor
    FROM intermediate   

)

 SELECT * 
 FROM final