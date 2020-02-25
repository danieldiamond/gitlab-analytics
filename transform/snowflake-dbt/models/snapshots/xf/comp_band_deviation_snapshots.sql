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
         IFF(nullif(deviation_from_comp_calc, '') = 'Exec', '0.0%',
            nullif(deviation_from_comp_calc, '')) as deviation_from_comp_calc_cl,
         CASE WHEN "DBT_VALID_FROM"::number::timestamp::date < '2019-10-18'::date
             THEN '2000-01-20'::date
             ELSE "DBT_VALID_FROM"::number::timestamp::date END AS valid_from,
         "DBT_VALID_TO"::number::timestamp::date                AS valid_to
    FROM source
    WHERE deviation_from_comp_calc_cl IS NOT NULL

  ), deduplicated as (

      SELECT distinct
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
