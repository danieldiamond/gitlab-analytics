WITH unioned AS (
 
  SELECT *
  FROM {{ ref('self_managed_free_to_paid') }}

  UNION

  SELECT *
  FROM {{ ref('saas_free_to_paid') }}
  
)

SELECT *
FROM unioned
