{{ config({
        "materialized": "incremental",
        "unique_key": "ip_address_hash",
        "schema": "analytics"
    })
}}

{% if execute %}
  {% if flags.FULL_REFRESH %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model. Exclude it from the run via the argument \"--exclude staging.common.dim_ip_to_geo\".") }}
  {% endif %}
{% endif %}

WITH all_hashed_ips_version_usage AS (

    SELECT
      {{ hash_sensitive_columns('version_usage_data') }}
    FROM {{ ref('version_usage_data') }}

),  all_distinct_ips AS (

    SELECT DISTINCT 
      source_ip_hash, 
      PARSE_IP(source_ip, 'inet')['ip_fields'][0]::NUMBER AS source_ip_numeric 
    FROM all_hashed_ips_version_usage
    {% if is_incremental() %}
        WHERE source_ip_hash NOT IN (
            SELECT 
            source_ip_hash 
            FROM {{this}}
        )
    {% endif %}

), maxmind_ip_ranges AS (

   SELECT *
   FROM {{ ref('maxmind_ip_ranges_source') }}

), newly_mapped_ips AS (

    SELECT 
      source_ip_hash,
      location_id
    FROM all_distinct_ips
    LEFT JOIN maxmind_ip_ranges
    WHERE all_distinct_ips.source_ip_numeric BETWEEN maxmind_ip_ranges.ip_range_first_ip_numeric AND maxmind_ip_ranges.ip_range_last_ip_numeric 

)
SELECT *
FROM newly_mapped_ips