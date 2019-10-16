WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('pings_tap_postgres', 'usage_data') }}

),

renamed AS (

  SELECT
    id::INTEGER                              AS id,
    source_ip::VARCHAR                       AS source_ip,
    version::VARCHAR                         AS version,
    active_user_count::INTEGER               AS active_user_count,
    license_md5::VARCHAR                     AS license_md5,
    historical_max_users::INTEGER            AS historical_max_users,
    --licensee // removed for PII
    license_user_count::INTEGER              AS license_user_count,
    license_starts_at::TIMESTAMP             AS license_starts_at,
    license_expires_at::TIMESTAMP            AS license_expires_at,
    PARSE_JSON(license_add_ons)              AS license_add_ons,
    license_restricted_user_count::INTEGER   AS license_restricted_user_count,
    recorded_at::TIMESTAMP                   AS recorded_at,
    created_at::TIMESTAMP                    AS created_at,
    updated_at::TIMESTAMP                    AS updated_at,
    license_id::INTEGER                      AS license_id,
    stats                                    AS stats_raw,
    mattermost_enabled::BOOLEAN              AS mattermost_enabled,
    uuid::VARCHAR                            AS uuid,
    edition::VARCHAR                         AS edition,
    hostname::VARCHAR                        AS hostname,
    host_id::INTEGER                         AS host_id,
    license_trial::BOOLEAN                   AS license_trial,
    source_license_id::INTEGER               AS source_license_id,
    installation_type::VARCHAR               AS installation_type,
    license_plan::VARCHAR                    AS license_plan,
    database_adapter::VARCHAR                AS database_adapter,
    database_version::VARCHAR                AS database_version,
    git_version::VARCHAR                     AS git_version,
    gitlab_pages_enabled::BOOLEAN            AS gitlab_pages_enabled,
    gitlab_pages_version::VARCHAR            AS gitlab_pages_version,
    container_registry_enabled::BOOLEAN      AS container_registry_enabled,
    elasticsearch_enabled::BOOLEAN           AS elasticsearch_enabled,
    geo_enabled::BOOLEAN                     AS geo_enabled,
    gitlab_shared_runners_enabled::BOOLEAN   AS gitlab_shared_runners_enabled,
    gravatar_enabled::BOOLEAN                AS gravatar_enabled,
    ldap_enabled::BOOLEAN                    AS ldap_enabled,
    omniauth_enabled::BOOLEAN                AS omniauth_enabled,
    reply_by_email_enabled::BOOLEAN          AS reply_by_email_enabled,
    signup_enabled::BOOLEAN                  AS signup_enabled,
    web_ide_commits::INTEGER                 AS web_ide_commits,
    influxdb_metrics_enabled::BOOLEAN        AS influxdb_metrics_enabled,
    prometheus_metrics_enabled::BOOLEAN      AS prometheus_metrics_enabled,
    --smau // never not null
    --usage_activity_by_stage // never not null
    gitaly_version::VARCHAR                  AS gitaly_version,
    gitaly_servers::INTEGER                  AS gitaly_servers,
    -- gitaly_filesystems::VARCHAR // waiting on fresh data https://gitlab.com/gitlab-data/analytics/issues/2696
    PARSE_JSON(counts)                       AS stats_used
  FROM source
  WHERE uuid IS NOT NULL
    AND (CHECK_JSON(counts) IS NULL)
    AND rank_in_key = 1
)

SELECT *
FROM renamed
