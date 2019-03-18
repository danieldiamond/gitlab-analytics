WITH source as (

    SELECT *
    FROM {{ var("database") }}.gcloud_postgres_stitch.version_usage_data
)

SELECT active_user_count,
		created_at,
		historical_max_users,
		id, 
		license_expires_at,
		license_id,
		license_starts_at,
		license_user_count,
		recorded_at,
		source_ip,
		stats,
		updated_at,
		version,
		hostname,
		counts,
		uuid,
		edition,
		license_add_ons,
		mattermost_enabled,
		host_id,
		installation_type, 
		parse_json(stats) as stats_used
FROM source
WHERE uuid IS NOT NULL
AND created_at < '2019-01-20'