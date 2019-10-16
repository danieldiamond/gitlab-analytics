{{ config({
    "schema": "sensitive",
    "materialized": "table"
    })
}}

with source as (

    SELECT *
    FROM {{ source('snapshots', 'sheetload_employee_location_factor_snapshots') }}
    WHERE dbt_scd_id != 'fd3dee5ab322f68692b97130f2bfefab'
    AND "Employee_ID" != 'Not In Comp Calc'
    AND "Employee_ID" NOT IN ('$72,124')

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
        valid_to
FROM renamed
)
SELECT bamboo_employee_number,
       location_factor,
       min(valid_from) as valid_from,
       max(valid_to) as valid_to
FROM deduplicated
GROUP BY 1, 2
