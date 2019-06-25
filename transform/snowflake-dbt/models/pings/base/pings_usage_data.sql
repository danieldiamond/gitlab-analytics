WITH source AS (

  SELECT *
  FROM {{ source('pings_tap_postgres', 'usage_data') }}

)

SELECT  id,

        created_at,
        recorded_at,
        updated_at,

        license_expires_at,
        license_id,
        license_starts_at,
        license_user_count,
        license_add_ons,

        source_ip,
        hostname,
        host_id,
        uuid,

        version,
        edition,
        installation_type,

        stats,
        counts,
        mattermost_enabled,
        active_user_count,
        historical_max_users,

        parse_json(counts) as stats_used

FROM source
WHERE uuid IS NOT NULL