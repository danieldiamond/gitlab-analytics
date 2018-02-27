with libre_agg as (
  SELECT * FROM {{ ref('libre_agg_hosts') }}
)

SELECT
  a.id,
  coalesce(a.website, 'www.' || company_domain)     AS website,
  a.name,
  CASE WHEN a.type = 'Prospect'
    THEN 'Prospect - CE User'
  ELSE a.type END                                   AS type,
  a.billingstate,
  a.billingcountry,
  TRUE                                              AS Using_CE__c,
  COALESCE(a.Industry, company_industry)            AS Industry,
  coalesce(a.numberofemployees, CASE WHEN employees = 0
    THEN NULL
                                ELSE employees END) AS emmployees,
  CE_Hosts__c,
  CE_Users__c
FROM sfdc.account a INNER JOIN (
                                 SELECT
                                   company_name,
                                   clean_url                      AS company_domain,
                                   max(CASE WHEN company_type = 'Education'
                                     THEN 'Education'
                                       ELSE company_industry END) AS company_industry,
                                   max(CASE WHEN employees IS NOT NULL
                                     THEN employees :: INTEGER
                                       ELSE 0 END)                AS employees,
                                   max(hosts_count)               AS CE_Hosts__c,
                                   max(active_user_count)         AS CE_Users__c
                                 FROM libre_agg
                                 GROUP BY company_name, company_domain
                               ) d ON d.company_name = a.name
WHERE a.isdeleted = FALSE AND ((d.CE_Hosts__c <> a.ce_instances__c OR a.ce_instances__c IS NULL) AND
                               (d.CE_Users__c <> a.active_ce_users__c OR a.active_ce_users__c IS NULL)) AND
      a.name NOT IN ('Microsoft', 'Amazon.com')
ORDER BY d.CE_Hosts__c DESC