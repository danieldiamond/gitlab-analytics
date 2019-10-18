{{ config({
    "schema": "sensitive",
    "materialized": "table"
    })
}}

with source as (

    SELECT *
    FROM {{ source('snapshots', 'sheetload_employee_location_factor_snapshots') }}
    WHERE dbt_scd_id NOT IN ('fd3dee5ab322f68692b97130f2bfefab', '99db544c0fd2a218011077d77c7654ed')
    AND "Employee_ID" != 'Not In Comp Calc'
    AND "Employee_ID" NOT IN ('$72,124')

), renamed as (

    SELECT
         nullif("Employee_ID",'')::varchar as bamboo_employee_number,
         IFF(nullif(deviation_from_comp_calc, '') = 'Exec', 0,
            nullif(deviation_from_comp_calc, '')) as deviation_from_comp_calc_cl,
         CASE WHEN "DBT_VALID_FROM"::number::timestamp::date < '2019-10-18'::date
             THEN '2000-01-20'::date
             ELSE "DBT_VALID_FROM"::number::timestamp::date END AS valid_from,
         "DBT_VALID_TO"::number::timestamp::date                AS valid_to
    FROM source
    WHERE lower(bamboo_employee_number) NOT LIKE '%not in comp calc%'
    AND deviation_from_comp_calc_cl IS NOT NULL

  ), deduplicated as (

      SELECT
        bamboo_employee_number::bigint as bamboo_employee_number,
        round(deviation_from_comp_calc_cl::float, 2) as deviation_from_comp_calc,
        valid_from,
        valid_to
      FROM renamed
  ), final as (

  SELECT
    bamboo_employee_number,
    deviation_from_comp_calc,
    min(valid_from) as valid_from,
    NULLIF(max(valid_to), CURRENT_DATE) as valid_to
  FROM deduplicated
  GROUP BY 1, 2

  )

  SELECT *
  FROM final
