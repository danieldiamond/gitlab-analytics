WITH  greenhouse_metrics_unpivoted AS (
    
     {{ dbt_utils.unpivot(relation=ref('greenhouse_sourcer_base'), cast_to='varchar', 
            exclude=['reporting_month', 'start_period','end_period','sourcer_name']) }}
   
)

SELECT * 
FROM greenhouse_metrics_unpivoted
