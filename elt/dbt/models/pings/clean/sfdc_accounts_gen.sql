with libre_agg as (
  SELECT * FROM {{ ref('libre_agg_hosts') }}
)

SELECT
  max(company_name)            AS name,
  the_clean_url                AS website,
  'Prospect - CE User' :: TEXT AS type,
  '00561000000mpHTAAY' :: TEXT AS OwnerId,
  'True' :: BOOLEAN            AS using_ce__c
FROM libre_agg AS lah
  LEFT OUTER JOIN
  (
    SELECT
      id,
      website                                              AS original,
      regexp_replace(website, '^(http(s)?\://)?www\.', '') AS fixed
    FROM sfdc.account
    WHERE website IS NOT NULL) AS sf
    ON lah.the_clean_url = sf.fixed
WHERE sf.fixed IS NULL
      AND the_clean_url !~ '\d+\.\d+.\d+\.\d+'
      AND lah.company_name NOT IN ('Microsoft', 'Amazon.com')
GROUP BY the_clean_url

UNION

SELECT
  max(company_name)            AS name,
  the_clean_url                AS website,
  'Prospect - CE User' :: TEXT AS type,
  '00561000000mpHTAAY' :: TEXT AS OwnerId,
  'True' :: BOOLEAN            AS using_ce__c
FROM libre_agg AS lah
  LEFT OUTER JOIN
  (
    SELECT
      id,
      name
    FROM sfdc.account) AS sf
    ON lah.company_name = sf.name
WHERE sf.name IS NULL
      AND the_clean_url !~ '\d+\.\d+.\d+\.\d+'
      AND lah.company_name NOT IN ('Microsoft', 'Amazon.com')
GROUP BY the_clean_url
ORDER BY name