{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('version', 'usage_data') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})
    {% endif %}

    --TEMP
    --QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    LIMIT 100000

    

),

renamed AS (

    SELECT
        id::INTEGER                                  AS id,
        source_ip::VARCHAR                           AS source_ip,
        version::VARCHAR                             AS version,
        active_user_count::INTEGER                   AS active_user_count,
        license_md5::VARCHAR                         AS license_md5,
        historical_max_users::INTEGER                AS historical_max_users,
        --licensee // removed for PII
        license_user_count::INTEGER                  AS license_user_count,
        license_starts_at::TIMESTAMP                 AS license_starts_at,
        license_expires_at::TIMESTAMP                AS license_expires_at,
        PARSE_JSON(license_add_ons)                  AS license_add_ons,
        recorded_at::TIMESTAMP                       AS recorded_at,
        created_at::TIMESTAMP                        AS created_at,
        updated_at::TIMESTAMP                        AS updated_at,
        license_id::INTEGER                          AS license_id,
        stats                                        AS stats_raw,
        mattermost_enabled::BOOLEAN                  AS mattermost_enabled,
        uuid::VARCHAR                                AS uuid,
        edition::VARCHAR                             AS edition,
        hostname::VARCHAR                            AS hostname,
        host_id::INTEGER                             AS host_id,
        license_trial::BOOLEAN                       AS license_trial,
        source_license_id::INTEGER                   AS source_license_id,
        installation_type::VARCHAR                   AS installation_type,
        license_plan::VARCHAR                        AS license_plan,
        database_adapter::VARCHAR                    AS database_adapter,
        database_version::VARCHAR                    AS database_version,
        git_version::VARCHAR                         AS git_version,
        gitlab_pages_enabled::BOOLEAN                AS gitlab_pages_enabled,
        gitlab_pages_version::VARCHAR                AS gitlab_pages_version,
        container_registry_enabled::BOOLEAN          AS container_registry_enabled,
        elasticsearch_enabled::BOOLEAN               AS elasticsearch_enabled,
        geo_enabled::BOOLEAN                         AS geo_enabled,
        gitlab_shared_runners_enabled::BOOLEAN       AS gitlab_shared_runners_enabled,
        gravatar_enabled::BOOLEAN                    AS gravatar_enabled,
        ldap_enabled::BOOLEAN                        AS ldap_enabled,
        omniauth_enabled::BOOLEAN                    AS omniauth_enabled,
        reply_by_email_enabled::BOOLEAN              AS reply_by_email_enabled,
        signup_enabled::BOOLEAN                      AS signup_enabled,
        --web_ide_commits // was implemented as both a column and in `counts`
        influxdb_metrics_enabled::BOOLEAN            AS influxdb_metrics_enabled,
        prometheus_metrics_enabled::BOOLEAN          AS prometheus_metrics_enabled,
        --smau // never not null
        PARSE_JSON(usage_activity_by_stage)          AS usage_activity_by_stage,
        PARSE_JSON(usage_activity_by_stage_monthly)  AS usage_activity_by_stage_monthly,
        gitaly_version::VARCHAR                      AS gitaly_version,
        gitaly_servers::INTEGER                      AS gitaly_servers,
        gitaly_filesystems::VARCHAR                  AS gitaly_filesystems,
        PARSE_JSON(counts)                           AS stats_used
    FROM source
    WHERE CHECK_JSON(counts) IS NULL

)

SELECT *
FROM renamed
ORDER BY updated_at
