WITH row_count_calc AS (

  SELECT 
    COUNT(*)
  FROM {{ ref('zuora_subscription_periods') }}
  WHERE 
    (
      subscription_version_term_start_date IS NULL OR
      subscription_version_term_end_date IS NULL  
    )
    AND zuora_rate_plan_charge_id NOT IN (
      '2c92a0fe55a0e4a50155dc63317a53f7',
      '2c92a0fe5e8337ac015e9ad07fbc4d3c',
      '2c92a0ff567e3612015699201e7e0c81',
      '2c92a0fe5b1ae79b015b4080bb6d6b6a',
      '2c92a0ff5d2b864a015d3760ff681dc3'
    )

)

SELECT *
FROM row_count_calc
WHERE row_count > 1
