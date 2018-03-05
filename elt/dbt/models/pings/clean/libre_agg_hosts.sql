WITH libre AS (
    SELECT *
    FROM {{ ref('libre_hosts') }}
),

dorg_joined AS (
  SELECT
    dorg.company_name,
    dorg.company_industry,
    CASE WHEN dorg.company_type = 'Educational'
      THEN 'Education'
    ELSE initcap(dorg.company_type) END AS company_type,
    CASE WHEN dorg.company_emp ~ '\d+'
      THEN company_emp :: INTEGER
    ELSE 0 :: INTEGER END               AS employees,
    lh.clean_domain                     AS the_clean_url,
    lh.clean_full_domain                AS full_hostname,
    lh.ping_type,
    lh.raw_domain,
    lh.gitlab_version,
    lh.ping_date,
    lh.host_id,
    lh.ping_json_data :: TEXT,
    lh.max_active_user_count,
    lh.gitlab_edition,
    lh.license_ids,
    lh.mattermost_enabled,
    'DiscoverOrg' :: TEXT               AS source
  FROM
    libre AS lh
    JOIN discoverorg_cache AS dorg ON lh.clean_domain = dorg.domain
  WHERE dorg.company_name IS NOT NULL),

cbit_joined AS (
  SELECT
    cbit.company_name,
    cbit.company_industry,
    initcap(company_type),
    CASE WHEN cbit.company_emp ~ '\d+'
      THEN company_emp :: INTEGER
    ELSE 0 :: INTEGER END AS employees,
    dorg_remainder.*,
    'Clearbit' :: TEXT    AS source
  FROM
    (
      SELECT
        v.clean_domain      AS the_clean_url,
        v.clean_full_domain AS full_hostname,
        v.ping_type,
        v.raw_domain,
        v.gitlab_version,
        v.ping_date,
        v.host_id,
        v.ping_json_data :: TEXT,
        v.max_active_user_count,
        v.gitlab_edition,
        v.license_ids,
        v.mattermost_enabled
      FROM libre AS v
        LEFT OUTER JOIN dorg_joined ON v.clean_domain = dorg_joined.the_clean_url
      WHERE dorg_joined.the_clean_url ISNULL
    ) dorg_remainder
    JOIN clearbit_cache AS cbit ON dorg_remainder.the_clean_url = cbit.domain
  WHERE cbit.company_name IS NOT NULL),

whois_joined AS (
  SELECT
    whois.name      AS company_name,
    '' :: TEXT      AS company_industry,
    '' :: TEXT      AS company_type,
    '0' :: INTEGER  AS employees,
    cbit_remainder.*,
    'WHOIS' :: TEXT AS source
  FROM
    (
      SELECT
        v.clean_domain AS the_clean_url,
        v.clean_full_domain AS full_hostname,
        v.ping_type,
        v.raw_domain,
        v.gitlab_version,
        v.ping_date,
        v.host_id,
        v.ping_json_data :: TEXT,
        v.max_active_user_count,
        v.gitlab_edition,
        v.license_ids,
        v.mattermost_enabled
      FROM libre AS v
        LEFT OUTER JOIN dorg_joined ON v.clean_domain = dorg_joined.the_clean_url
        LEFT OUTER JOIN cbit_joined ON v.clean_domain = cbit_joined.the_clean_url
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
