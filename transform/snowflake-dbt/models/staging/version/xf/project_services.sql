WITH usage_data_unpacked_intermediate AS (

    SELECT *
    FROM {{  ref('version_usage_data_unpacked_intermediate') }}

), transformed AS (

    SELECT
      id,
      version,
      created_at,
      license_id,
      uuid,
      edition,      
      ping_source,
      major_version,
      main_edition,
      edition_type,
      license_plan_code,
      company,
      zuora_subscription_id,
      zuora_subscription_status,
      zuora_crm_id,
      created_at,
      edition,
      {{ star_regex(from=ref('version_usage_data_unpacked_intermediate'), except=(version_usage_stats_list|upper)) }}
    FROM usage_data_unpacked_intermediate

)

SELECT *
FROM transformed
