{{ config({
    "schema": "sensitive",
    "materialized": "table"
    })
}}

{% set max_date_in_analysis = "date_trunc('week', dateadd(week, 3, CURRENT_DATE))" %}

with source as (

  SELECT *
  FROM {{ref("comp_band_loc_factor_base")}}

), renamed as (

    SELECT
         nullif("Employee_ID",'')::varchar as bamboo_employee_number,
         nullif("Location_Factor",'') as location_factor,
         CASE WHEN "DBT_VALID_FROM"::number::timestamp::date < '2019-07-20'::date
             THEN '2000-01-20'::date
             ELSE "DBT_VALID_FROM"::number::timestamp::date END AS valid_from,
         "DBT_VALID_TO"::number::timestamp::date                AS valid_to
    FROM source
    WHERE lower(bamboo_employee_number) NOT LIKE '%not in comp calc%'
    AND location_factor IS NOT NULL


), employee_locality AS (

    SELECT *
    FROM {{ ref('employee_locality') }}
    
), unioned AS (

    SELECT 
      bamboo_employee_number::bigint as bamboo_employee_number,
      null                           AS locality,
      (location_factor::float)*100   as location_factor,
      valid_from,
      valid_to
    FROM renamed
    WHERE valid_from < '2020-03-24'
    ---from 2020.03.24 we start capturing this data from bamboohr

    UNION ALL
    
    SELECT 
      employee_number,
      bamboo_locality,
      location_factor,
      updated_at,
      LEAD(updated_at) OVER (PARTITION BY employee_number ORDER BY updated_at) AS valid_to
    FROM employee_locality

), deduplicated as (

SELECT 
  bamboo_employee_number::bigint as bamboo_employee_number,
  locality,
  location_factor::float as location_factor,
  valid_from,
  COALESCE(valid_to, {{max_date_in_analysis}}) as valid_to, 
  conditional_change_event(location_factor) over(order by bamboo_employee_number,locality, valid_to asc) as location_factor_change_event_number
FROM unioned

)

SELECT bamboo_employee_number,
       location_factor,
       locality,
       location_factor_change_event_number,
       min(valid_from) as valid_from,
       max(DATEADD(day,-1,valid_to)) as valid_to
FROM deduplicated
---starting on this date we start capturing location factor in bamboohr
GROUP BY 1, 2, 3, 4


