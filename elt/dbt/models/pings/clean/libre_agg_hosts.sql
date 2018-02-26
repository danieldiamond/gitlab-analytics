with libre as (
  SELECT * FROM {{ ref('libre_hosts') }}
)


SELECT
   dorg.company_name,
   dorg.company_industry,
   CASE WHEN dorg.company_type = 'Educational'
     THEN 'Education'
       ELSE initcap(dorg.company_type) END AS company_type,
   CASE WHEN dorg.company_emp ~ '\d+'
     THEN company_emp :: INTEGER
   ELSE 0 END    AS employees,
   libre.*,
   'DiscoverOrg' AS source
 FROM
   libre
   JOIN discoverorg_cache AS dorg ON libre.clean_url = dorg.domain
                                     AND dorg.last_update :: DATE >= (now() - '30 days' :: INTERVAL)
 WHERE dorg.company_name IS NOT NULL

UNION

SELECT
   cbit.company_name,
   cbit.company_industry,
   initcap(company_type),
   CASE WHEN cbit.company_emp ~ '\d+'
     THEN company_emp :: INTEGER
   ELSE 0 END AS employees,
   libre.*,
   'Clearbit' AS source
 FROM
   libre
   JOIN clearbit_cache AS cbit ON libre.clean_url = cbit.domain
                                  AND cbit.last_update :: DATE >= (now() - '30 days' :: INTERVAL)
 WHERE cbit.company_name IS NOT NULL

UNION

SELECT
   whois.name AS company_name,
   NULL       AS company_industry,
   NULL       AS company_type,
   0          AS employees,
   libre.*,
   'WHOIS'    AS source
 FROM
   libre
   JOIN whois_cache AS whois ON libre.clean_url = whois.domain
                                AND whois.last_update :: DATE >= (now() - '30 days' :: INTERVAL)
 WHERE whois.name IS NOT NULL