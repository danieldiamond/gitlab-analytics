WITH usage_pings AS (
    SELECT *
    FROM {{ ref('usage_data_clean') }}
),

vers_pings AS (
  SELECT *
  FROM {{ ref('version_checks_clean') }}
  ),

  --   Combine version and usage pings together
libre_join AS (
  SELECT
    udc.clean_url          AS clean_domain,
    'Usage' :: TEXT        AS ping_type,
    udc.raw_domain         AS raw_domain,
    udc.version            AS gitlab_version,
    udc.updated_at         AS ping_date,
    udc.host_id            AS host_id,
    udc.stats              AS ping_json_data,
    udc.active_user_count  AS active_user_count,
    udc.edition            AS gitlab_edition,
    udc.license_id         AS license_id,
    udc.mattermost_enabled AS mattermost_enabled


  FROM usage_pings AS udc

  UNION ALL

  SELECT
    vcc.clean_url            AS clean_domain,
    'Version' :: TEXT        AS ping_type,
    vcc.referer_url          AS raw_domain,
    vcc.gitlab_version       AS gitlab_version,
    vcc.updated_at           AS ping_date,
    vcc.host_id              AS host_id,
    vcc.request_data :: JSON AS ping_json_data,
    NULL                     AS active_user_count,
    NULL                     AS gitlab_edition,
    NULL                     AS license_id,
    FALSE                    AS mattermost_enabled
  FROM vers_pings AS vcc
)

SELECT
  clean_domain,
  ping_type,
  raw_domain,
  gitlab_version,
  max(ping_date)                       AS ping_date,
  host_id,
  max(ping_json_data :: TEXT) :: JSON  AS ping_json_data,
  max(active_user_count)               AS max_active_user_count,
  max(gitlab_edition)                     gitlab_edition,
  string_agg(license_id :: TEXT, ', ') AS license_ids,
  mattermost_enabled
FROM libre_join
GROUP BY clean_domain,
  ping_type,
  raw_domain,
  gitlab_version,
  host_id,
  mattermost_enabled