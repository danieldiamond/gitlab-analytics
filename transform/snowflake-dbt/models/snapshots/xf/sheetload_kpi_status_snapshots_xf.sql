WITH source AS (

    SELECT *
    FROM {{ref("sheetload_kpi_status_snapshots_base")}}

), dates AS (

    SELECT *
    FROM {{ref("date_details")}}
    WHERE date_actual > '2020-03-30'
    AND date_actual <= CURRENT_DATE

  ), joined AS (

    SELECT dates.date_actual, source.*
    FROM dates
    LEFT JOIN source
    ON dates.date_actual >= dbt_valid_from
    AND dates.date_actual < COALESCE(dbt_valid_to, CURRENT_DATE)
    FROM renamed

  )

  SELECT *
  FROM joined
