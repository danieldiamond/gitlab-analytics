with libre_agg as (
  SELECT * FROM {{ ref('libre_agg_hosts') }}
),

libre AS (
  SELECT *
  FROM {{ ref('libre_hosts') }}
),

host_data AS (
    SELECT *
    FROM libre
    JOIN libre_agg ON libre.clean_domain = libre_agg.the_clean_url
  )

SELECT
  max(company_name)            AS name,
  clean_domain                 AS website,
  'Prospect - CE User' :: TEXT AS type,
  '00561000000mpHTAAY' :: TEXT AS OwnerId,
  'True' :: BOOLEAN            AS using_ce__c
FROM host_data AS lah
  LEFT OUTER JOIN
  (
    SELECT
      id,
      website                                              AS original,
      regexp_replace(website, '^(http(s)?\://)?www\.', '') AS fixed
    FROM sfdc.account
    WHERE website IS NOT NULL
  ) AS sf
    ON lah.clean_domain = sf.fixed
WHERE sf.fixed IS NULL
      AND clean_domain !~ '\d+\.\d+.\d+\.\d+'
      AND lah.company_name NOT IN ('Microsoft', 'Amazon.com')
GROUP BY clean_domain

UNION

SELECT
  max(company_name)            AS name,
  clean_domain                 AS website,
  'Prospect - CE User' :: TEXT AS type,
  '00561000000mpHTAAY' :: TEXT AS OwnerId,
  'True' :: BOOLEAN            AS using_ce__c
FROM host_data AS lah
  LEFT OUTER JOIN
  (
    SELECT
      id,
      name
    FROM sfdc.account) AS sf
    ON lah.company_name = sf.name
WHERE sf.name IS NULL
      AND clean_domain !~ '\d+\.\d+.\d+\.\d+'
      AND lah.company_name NOT IN ('Microsoft', 'Amazon.com')
GROUP BY clean_domain
ORDER BY name