WITH unioned AS (
  
    SELECT *
    FROM {{ ref('self_managed_direct_conversion') }}

    UNION

    SELECT *
    FROM {{ ref('saas_direct_conversion') }}

)

SELECT *
FROM unioned
