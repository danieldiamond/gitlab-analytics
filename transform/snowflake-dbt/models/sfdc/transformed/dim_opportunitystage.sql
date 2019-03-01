{{
  config(
    materialized='table',
    schema='analytics'
  )
}}

WITH stages AS (
  SELECT * FROM {{ ref("sfdc_opportunitystage") }}
)

SELECT
    *
FROM stages