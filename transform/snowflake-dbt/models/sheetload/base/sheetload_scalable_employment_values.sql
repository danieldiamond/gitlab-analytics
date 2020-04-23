WITH source AS (

    SELECT *
    FROM {{ source('sheetload','scalable_employment_values') }}

)

SELECT
  "Month"::DATE                             AS nbr_month,
  NULLIF(TOTAL_WORKFORCE, '')::INT          AS total_workforce,
  NULLIF(NBR_IN_SCALABLE_SOLUTION, '')::INT AS nbr_in_scalable_solution,
  NULLIF(NBR_IN_PROCESS, ''):: INT          AS nbr_in_process,
  NULLIF(NBR_TO_BE_CONVERTED, '')::INT      AS nbr_to_be_converted
FROM source