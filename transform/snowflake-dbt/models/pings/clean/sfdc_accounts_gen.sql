-- This generates the records that need to be uploaded to SFDC as new accounts
{% set ip_pattern = "'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'"%}

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
),

name_match AS (
/*
This takes host files (non IP), matches to SFDC on name, and gets those
host files (uniquely identified by the clean_domain) where there is no name match in salesforce.
 */
  SELECT
    max(company_name)            AS name,
    clean_domain                 AS website
  FROM host_data AS lah
    LEFT OUTER JOIN
    (
      SELECT
        id,
        name,
        type
      FROM raw.salesforce_stitch.account) AS sf
      ON lah.company_name = sf.name
      AND sf.type != 'Customer'
  WHERE sf.name IS NULL
        AND NOT rlike(clean_domain, {{ ip_pattern }}, 'i')
        AND lah.company_name NOT IN ('Microsoft', 'Amazon.com')
  GROUP BY clean_domain
)

/*
Takes the remaining host records, attempts to match with SFDC based on website,
and returns those where there is no match.
 */
SELECT
  max(name)                    AS name,
  website                      AS website,
  'Prospect - CE User' :: TEXT AS type,
  '00561000000mpHTAAY' :: TEXT AS ownerid,
  'True' :: BOOLEAN            AS using_ce__c,
  'CE Download' :: TEXT        AS accountsource
FROM name_match AS lah
LEFT OUTER JOIN
  (
    SELECT
      sfdc.id,
      sfdc.type,
      sfdc.website                                              AS original,
      regexp_replace(sfdc.website, '^(http(s)?\://)?www\.', '') AS fixed
    FROM raw.salesforce_stitch.account AS sfdc
    WHERE website IS NOT NULL
  ) AS sf
    ON lah.website = sf.fixed
    AND sf.type != 'Customer'
WHERE sf.fixed IS NULL
      AND NOT rlike(website, {{ ip_pattern }}, 'i')
      AND lah.name NOT IN ('Microsoft', 'Amazon.com')
GROUP BY website

ORDER BY name

