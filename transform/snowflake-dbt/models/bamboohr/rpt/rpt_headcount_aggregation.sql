{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

With overall_headcount_pivoted AS (
   
    SELECT 
        DATE_TRUNC('month',month_date)                  AS month_date,
        {{ pivot_macro_in_bamboohr(
            'metric',
            pivot_macro_in_bamboohr.get_column_values(ref('bamboohr_headcount_aggregation_intermediate'), 'metric')
        ) }}

        {# {{ pivot_macro_in_bamboohr('metric', ('headcount_start','headcount_end')) }} #}


    FROM {{ ref('bamboohr_headcount_aggregation_intermediate') }}
    GROUP BY DATE_TRUNC('month',month_date)


)

SELECT * 
FROM overall_headcount_pivoted


      
      

