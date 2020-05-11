WITH  greenhouse_metrics_unpivoted AS (
    
     {{ dbt_utils.unpivot(relation=ref('greenhouse_sourcer_base'), cast_to='FLOAT', 
            exclude=['reporting_month', 'start_period','end_period','sourcer_name','month_date'])
     }}
   
)

SELECT 
  reporting_month,
  FIELD_NAME AS recruiting_metric,
  PERCENTILE_CONT(0.25) WITHIN group (order by VALUE)   AS quartile_one
  PERCENTILE_CONT(0.50) WITHIN group (order by VALUE)   AS quartile_two,
  PERCENTILE_CONT(0.75) WITHIN group (order by VALUE)   AS quartile_three,
  PERCENTILE_CONT(0.90) WITHIN group (order by VALUE)   AS ninetieth_percentile,
  PERCENTILE_CONT(1.00) WITHIN group (order by VALUE)   AS quartile_four 
FROM greenhouse_metrics_unpivoted
WHERE greenhouse_metrics_unpivoted.VALUE IS NOT NULL
GROUP BY 1,2