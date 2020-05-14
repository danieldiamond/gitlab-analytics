{{ config({
    "schema": "sensitive",
    "materialized": "table"
    })
}}

{% set max_date_in_analysis = "date_trunc('week', dateadd(week, 3, CURRENT_DATE))" %}

WITH source AS (

  SELECT *
  FROM {{ref("comp_band_loc_factor_base")}}

), renamed as (

    SELECT
    NULLIF("Employee_ID",'')::VARCHAR                       AS bamboo_employee_number,
    NULLIF("Location_Factor",'')                            AS location_factor,
    CASE WHEN "DBT_VALID_FROM"::NUMBER::TIMESTAMP::DATE < '2019-07-20'::DATE
             THEN '2000-01-20'::DATE
             ELSE "DBT_VALID_FROM"::NUMBER::TIMESTAMP::DATE END AS valid_from,
         "DBT_VALID_TO"::number::timestamp::DATE                AS valid_to
    FROM source
    WHERE LOWER(bamboo_employee_number) NOT LIKE '%not in comp calc%'
      AND location_factor IS NOT NULL

), employee_locality AS (

    SELECT *
    FROM {{ ref('employee_locality') }}
    
), unioned AS (

    SELECT 
      bamboo_employee_number::BIGINT AS bamboo_employee_number,
      NULL                           AS locality,
      (location_factor::FLOAT)*100   AS location_factor,
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

), intermediate AS (

    SELECT 
      bamboo_employee_number                                        AS bamboo_employee_number,
      locality,
      location_factor                                               AS location_factor,
      LEAD(location_factor) OVER 
          (PARTITION BY bamboo_employee_number ORDER BY valid_from) AS next_location_factor,
      valid_from,
      COALESCE(valid_to, {{max_date_in_analysis}})                  AS valid_to
    FROM unioned

), deduplicated AS (

    SELECT *
    FROM intermediate
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bamboo_employee_number, locality, location_factor, next_location_factor ORDER BY valid_from) =1

)

SELECT 
  bamboo_employee_number,
  locality,
  location_factor,
  valid_from                                                            AS valid_from,
  COALESCE( 
    LEAD(DATEADD(day,-1,valid_from)) 
    OVER (PARTITION BY bamboo_employee_number ORDER BY valid_from),
    {{max_date_in_analysis}})                                           AS valid_to
FROM deduplicated
GROUP BY 1, 2, 3, 4


