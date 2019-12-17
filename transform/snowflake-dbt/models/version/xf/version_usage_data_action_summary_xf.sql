WITH usage_data AS (

    SELECT *
    FROM {{ ref('version_usage_data') }}

), by_stage AS (

    SELECT
      data.id            AS usage_data_id,
      data.license_md5   AS license_md5,
      data.created_at    AS created_at,
      f.path,
      f.value            AS stage_json
    FROM usage_data AS data,
      LATERAL FLATTEN(INPUT => usage_activity_by_stage, RECURSIVE => False) f
    WHERE usage_activity_by_stage != '{}'

), final AS (

    SELECT
      by_stage.usage_data_id,
      by_stage.license_md5,
      by_stage.created_at,
      by_stage.path AS stage,
      f.path        AS action,
      f.value       AS count_users
    FROM by_stage,
      LATERAL FLATTEN(INPUT => by_stage.stage_json, RECURSIVE => False) f

)

SELECT *
FROM final
