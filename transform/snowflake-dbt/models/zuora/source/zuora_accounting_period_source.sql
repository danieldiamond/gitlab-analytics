WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'accounting_period') }}

), renamed AS (

    SELECT
      --Primary Keys
<<<<<<< HEAD
      id::VARCHAR                       AS accounting_period_id,

      --Info
      enddate::TIMESTAMP_TZ             AS end_date,
      fiscalyear::NUMBER                AS fiscal_year,
      name::VARCHAR                     AS accounting_period_name,
      startdate::TIMESTAMP_TZ           AS accounting_period_start_date,
      status::VARCHAR                   AS accounting_period_status,
      updatedbyid::VARCHAR              AS updated_by_id,
      updateddate::TIMESTAMP_TZ         AS updated_date
=======
      id::VARCHAR                     AS accounting_period_id,

      --Info
      enddate::TIMESTAMP_TZ           AS end_date,
      fiscalyear::NUMBER              AS fiscal_year,
      name::VARCHAR                   AS accounting_period_name,
      startdate::TIMESTAMP_TZ         AS accounting_period_start_date,
      status::VARCHAR                 AS accounting_period_status,
      updatedbyid::VARCHAR            AS updated_by_id,
      updateddate::TIMESTAMP_TZ       AS updated_date

>>>>>>> 7df100be211277b1c18b33e3fb5dae00989cc1a4
    FROM source

)

SELECT *
FROM renamed
