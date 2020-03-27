WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}

)

SELECT *
FROM filtered
WHERE is_deleted = FALSE
