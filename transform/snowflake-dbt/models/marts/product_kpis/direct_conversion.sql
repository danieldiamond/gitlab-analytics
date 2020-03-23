WITH unioned AS (
  
    SELECT *
    FROM {{ ref('self_managed_direct_conversion') }}

    UNION

    SELECT *
    FROM {{ ref('saas_direct_conversion') }}

)

, grouped AS (
  
SELECT 
    subscription_month,
    SUM(arr) AS arr
FROM unioned
GROUP BY 1

)

SELECT 
  *,
  (arr / LAG(arr, 1) OVER (PARTITION BY 1 ORDER BY subscription_month) - 1) AS pct_growt
FROM grouped
WHERE subscription_month >= '2019-01-01'
