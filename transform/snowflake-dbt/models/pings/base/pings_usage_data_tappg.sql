WITH source as (

    SELECT * FROM {{ var("database") }}.tap_postgres.version_db_usage_data
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

        parse_json(stats) as stats_used
FROM source
WHERE uuid IS NOT NULL
  AND created_at >= '2019-01-20'