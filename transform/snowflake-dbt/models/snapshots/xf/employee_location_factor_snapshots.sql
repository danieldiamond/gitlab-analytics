{{ config({
    "schema": "sensitive",
    "materialized": "table"
    })
}}

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

), deduplicated as (

SELECT bamboo_employee_number::bigint as bamboo_employee_number,
        location_factor::float as location_factor,
        valid_from,
        COALESCE(valid_to, CURRENT_DATE) as valid_to,
        conditional_change_event(location_factor) over(order by bamboo_employee_number, valid_to asc) as location_factor_change_event_number
FROM renamed
)
SELECT bamboo_employee_number,
       location_factor,
       location_factor_change_event_number,
       min(valid_from) as valid_from,
       max(valid_to) as valid_to
FROM deduplicated
GROUP BY 1, 2, 3
