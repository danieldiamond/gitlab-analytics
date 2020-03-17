{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=['']) %}

WITH usage_data AS (

    SELECT {{ dbt_utils.star(from=ref('version_usage_data'), except=['LICENSE_ID', 'LICENSE_STARTS_AT', 'LICENSE_EXPIRES_AT']) }}
    FROM {{ ref('version_usage_data_unpacked') }}

), unpacked_stage_json AS (

    SELECT
      *,
      f.key                                                                           AS stage_name,
      f.value                                                                         AS stage_activity_count_json

    FROM usage_data,
      lateral flatten(input => usage_data.usage_activity_by_stage) f
    WHERE IS_OBJECT(f.value) = TRUE
      AND stats_used IS NOT NULL
    {% if is_incremental() %}
        AND created_at > (SELECT max(created_at) FROM {{ this }})
    {% endif %}

), final AS (

    SELECT
      unpacked_stage_json.id,
      unpacked_stage_json.version,
      unpacked_stage_json.created_at,
      unpacked_stage_json.license_id,
      unpacked_stage_json.uuid,
      unpacked_stage_json.edition,      
      unpacked_stage_json.ping_source,
      unpacked_stage_json.major_version,
      unpacked_stage_json.main_edition,
      unpacked_stage_json.edition_type,
      unpacked_stage_json.license_plan_code,
      unpacked_stage_json.company,
      unpacked_stage_json.zuora_subscription_id,
      unpacked_stage_json.zuora_subscription_status,
      unpacked_stage_json.zuora_crm_id,
      unpacked_stage_json.stage_name,
      f.key      AS usage_action_name,
      f.value    AS usage_action_count
    FROM unpacked_stage_json,
      lateral flatten(input => unpacked_stage_json.stage_activity_count_json) f

)

SELECT *
FROM final
