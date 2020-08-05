{{ config({
    "schema": "sensitive",
    "materialized": "table"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ref("comp_band_loc_factor_base")}}

), renamed AS (

    SELECT
      NULLIF("Employee_ID",'')::VARCHAR                                     AS bamboo_employee_number,
      deviation_from_comp_calc                                              AS original_value_deviation_from_comp_calc,
      CASE 
        WHEN NULLIF(deviation_from_comp_calc, '') ='Exec'    
          THEN '0.00'
        WHEN NULLIF(deviation_from_comp_calc, '') ='#DIV/0!' 
          THEN NULL
        WHEN deviation_from_comp_calc LIKE '%'               
          THEN NULLIF(REPLACE(deviation_from_comp_calc,'%',''),'') 
        ELSE NULLIF(deviation_from_comp_calc, '') END                       AS deviation_from_comp_calc_cl,
      IFF("DBT_VALID_FROM"::NUMBER::TIMESTAMP::DATE < '2019-10-18'::date,
           '2000-01-20'::DATE,
           "DBT_VALID_FROM"::NUMBER::TIMESTAMP::DATE)                       AS valid_from,
      "DBT_VALID_TO"::NUMBER::TIMESTAMP::DATE                               AS valid_to
    FROM source
    WHERE deviation_from_comp_calc_cl IS NOT NULL

  ), deduplicated AS (

    SELECT DISTINCT   
      bamboo_employee_number::NUMBER                                       AS bamboo_employee_number,
      IFF(CONTAINS(original_value_deviation_from_comp_calc,'%') = True,
          ROUND(deviation_from_comp_calc_cl/100::FLOAT, 2),
          ROUND(deviation_from_comp_calc_cl::FLOAT, 2))                    AS deviation_from_comp_calc,
    valid_from,
    valid_to
    FROM renamed

  ), final AS (

  SELECT
    bamboo_employee_number,
    deviation_from_comp_calc,
    MIN(valid_from)                     AS valid_from,
    NULLIF(MAX(valid_to), CURRENT_DATE) AS valid_to
  FROM deduplicated
  GROUP BY 1, 2

  )

  SELECT *
  FROM final
