with source as (  
  SELECT *
  FROM {{ ref('location_factors_historical_greenhouse') }}

)

SELECt * from source