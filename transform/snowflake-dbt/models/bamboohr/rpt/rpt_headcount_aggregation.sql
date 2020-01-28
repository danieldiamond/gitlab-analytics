{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

With overall_headcount_pivoted AS (
   
    SELECT 
        DATE_TRUNC('month',month_date)                  AS month_date,  
        'total'                                         AS diversity_field,   
        'total'                                         AS aggregation_type,  
        {{ dbt_utils.pivot(
            'metric',
            dbt_utils.get_column_values(ref('bamboohr_headcount_aggregation_intermediate'), 'metric'),
            then_value ='total_count',
            quote_identifiers = False
        ) }} 
    FROM {{ ref('bamboohr_headcount_aggregation_intermediate') }}
    GROUP BY 1,2,3

), gender_headcount_pivoted AS (

    SELECT 
        DATE_TRUNC('month',month_date)                  AS month_date,  
        gender,    
        'gender_breakdown'                              AS aggregation_type,                                     
        {{ dbt_utils.pivot(
            'metric',
            dbt_utils.get_column_values(ref('bamboohr_headcount_aggregation_intermediate'), 'metric'),
            then_value ='total_count',
            quote_identifiers = False
        ) }} 
    FROM {{ ref('bamboohr_headcount_aggregation_intermediate') }}
    GROUP BY 1,2,3

), aggregated AS (

    SELECT
        overall_headcount_pivoted.*,
        (headcount_start + headcount_end)/2                                 AS average_headcount
    FROM overall_headcount_pivoted

    UNION ALL

    SELECT
        gender_headcount_pivoted.*,
        (headcount_start + headcount_end)/2                                 AS average_headcount
    FROM gender_headcount_pivoted

), final AS(

    SELECT 
      aggregated.month_date,
      aggregated.diversity_field,
      aggregated.aggregation_type,
      aggregated.headcount_start,
      aggregated.headcount_end,  
      aggregated.hires,
      AVG(a2.average_headcount)                                             AS rolling_12_month_headcount,    
      SUM(a2.total_separated)                                               AS rolling_12_month_separations,
      SUM(a2.voluntary_separations)                                         AS rolling_12_month_voluntary_separations,
      SUM(a2.involuntary_separations)                                       AS rolling_12_month_involuntary_separations,
      IFF(AVG(a2.average_headcount)<sum(a2.total_separated), null,
          1 -  (SUM(a2.total_separated) / avg(a2.average_headcount) ))      AS retention 
FROM aggregated
LEFT JOIN aggregated a2 
  ON a2.month_date BETWEEN DATEADD('month', -11, aggregated.month_date) AND aggregated.month_date
  AND a2.aggregation_type = aggregated.aggregation_type
  AND a2.diversity_field = aggregated.diversity_field
GROUP BY 1,2,3,4,5,6

)

SELECT *
FROM final