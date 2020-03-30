WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
