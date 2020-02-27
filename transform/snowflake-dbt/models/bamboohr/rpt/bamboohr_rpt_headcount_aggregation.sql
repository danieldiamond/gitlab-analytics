{{ config({
    "schema": "analytics"
    })
}}

 
{% set partition_statement = "OVER (PARTITION BY base.breakout_type, base.department, base.division, base.eeoc_field_name, base.eeoc_value
                              ORDER BY base.month_date DESC ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING)
                              " %}


with source AS (

    SELECT *
    FROM {{ ref ('bamboohr_headcount_intermediate_v2') }}

), base AS (

    SELECT DISTINCT 
      month_date,
      breakout_type, 
      department,
      division,
      eeoc_field_name,                                                       
      eeoc_value
    FROM source

)

   SELECT
      base.*,    
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
      IFF(rolling_12_month_headcount< rolling_12_month_separations, null,
        1 - (rolling_12_month_separations/NULLIF(rolling_12_month_headcount,0)))    AS retention, 
      {# RATIO_TO_REPORT(headcount_average) {{partition_statement}}                    AS percent_of_headcount,
      RATIO_TO_REPORT(hire_count) {{partition_statement}}                           AS percent_of_hires, #}

      headcount_average_leader,
      hired_leaders,
      separated_leaders,

      headcount_average_manager,
      hired_manager,
      separated_manager,

      headcount_average_contributor,
      hired_contributor,
      separated_contributor
    FROM base
    LEFT JOIN source  
      ON base.month_date = source.month_date
      AND base.breakout_type = source.breakout_type
      AND base.department = source.department
      AND base.division = source.division
      AND base.eeoc_field_name = source.eeoc_field_name
      AND base.eeoc_value = source.eeoc_value
    WHERE base.month_date < date_trunc('month', CURRENT_DATE)   