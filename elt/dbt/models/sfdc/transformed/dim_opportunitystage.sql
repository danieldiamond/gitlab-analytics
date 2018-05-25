{{
  config({
    "materialized":"table",
    "post-hook": [
       "ALTER TABLE {{ this }} ADD PRIMARY KEY(id)"
    ]
  })
}}

WITH mapped_stages AS (
  SELECT * FROM {{ ref("mapped_stages") }}
)

SELECT
  row_number()
  OVER (
    ORDER BY os.id )  AS id,
  sm.masterlabel, -- apinmae equals masterlabel as of 2018-05-24
  sm.mapped_stage     AS mapped_stage,
  defaultprobability,
  os.id               AS sfdc_id,
  isactive,
  isclosed,
  iswon
FROM sfdc.opportunitystage os
  JOIN mapped_stages sm ON sm.id = os.id