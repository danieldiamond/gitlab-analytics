{{
  config({
    "materialized":"table"
  })
}}

WITH stages AS (
  SELECT * FROM {{ ref("sfdc_opportunitystage") }}
)

SELECT
    *
FROM stages