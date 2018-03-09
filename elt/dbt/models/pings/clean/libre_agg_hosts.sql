WITH libre AS (
    SELECT *
    FROM {{ ref('libre_hosts') }}
),

    dorg_joined AS (
      SELECT
        dorg.company_name,
        libre.clean_domain                     AS the_clean_url,
        libre.clean_full_domain                AS clean_full_domain,
        libre.raw_domain,
        libre.gitlab_version,
        libre.ping_date,
        libre.host_id,
        libre.ping_usage_data :: TEXT,
        libre.ping_version_data :: TEXT,
        libre.active_user_count,
        libre.gitlab_edition,
        libre.license_id,
        libre.mattermost_enabled,
        'DiscoverOrg' :: TEXT               AS source
      FROM
        libre
        JOIN discoverorg_cache AS dorg ON libre.clean_domain = dorg.domain
      WHERE dorg.company_name IS NOT NULL),

    cbit_joined AS (
      SELECT
        cbit.company_name,
        dorg_remainder.*,
        'Clearbit' :: TEXT    AS source
      FROM
        (
          SELECT
            libre.clean_domain      AS the_clean_url,
            libre.clean_full_domain AS clean_full_domain,
            libre.raw_domain,
            libre.gitlab_version,
            libre.ping_date,
            libre.host_id,
            libre.ping_usage_data :: TEXT,
            libre.ping_version_data :: TEXT,
            libre.active_user_count,
            libre.gitlab_edition,
            libre.license_id,
            libre.mattermost_enabled
          FROM libre
            LEFT OUTER JOIN dorg_joined ON libre.clean_domain = dorg_joined.the_clean_url
          WHERE dorg_joined.the_clean_url ISNULL
        ) dorg_remainder
        JOIN clearbit_cache AS cbit ON dorg_remainder.the_clean_url = cbit.domain
      WHERE cbit.company_name IS NOT NULL),

    whois_joined AS (
      SELECT
        whois.name      AS company_name,
        cbit_remainder.*,
        'WHOIS' :: TEXT AS source
      FROM
        (
          SELECT
            libre.clean_domain      AS the_clean_url,
            libre.clean_full_domain AS clean_full_domain,
            libre.raw_domain,
            libre.gitlab_version,
            libre.ping_date,
            libre.host_id,
            libre.ping_usage_data :: TEXT,
            libre.ping_version_data :: TEXT,
            libre.active_user_count,
            libre.gitlab_edition,
            libre.license_id,
            libre.mattermost_enabled
          FROM libre
            LEFT OUTER JOIN dorg_joined ON libre.clean_domain = dorg_joined.the_clean_url
            LEFT OUTER JOIN cbit_joined ON libre.clean_domain = cbit_joined.the_clean_url
          WHERE dorg_joined.the_clean_url ISNULL AND
                cbit_joined.the_clean_url ISNULL
        ) AS cbit_remainder
        JOIN whois_cache AS whois ON cbit_remainder.the_clean_url = whois.domain
      WHERE whois.name IS NOT NULL
  )

SELECT *
FROM dorg_joined

UNION

SELECT *
FROM cbit_joined

UNION

SELECT *
FROM whois_joined
ORDER BY the_clean_url
