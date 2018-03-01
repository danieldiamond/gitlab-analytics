with libre_agg as (
  SELECT * FROM {{ ref('libre_agg_hosts') }}
)

SELECT
  a.id                                              AS Account__c,
  coalesce(a.website, 'www.' || company_domain)     AS host_domain__c,
  a.name                                            AS Host_Name__c,
  CASE WHEN a.type = 'Prospect'
    THEN 'Prospect - CE User'
  ELSE a.type END                                   AS Host_Type__c,
  a.billingstate                                    AS Billing_State__c,
  a.billingcountry                                  AS Billing_Country__c,
  COALESCE(a.Industry, company_industry)            AS Host_Industry__c,
  coalesce(a.numberofemployees, CASE WHEN employees = 0
    THEN NULL
                                ELSE employees END) AS Host_Employees__c,
  host_count                                        AS Host_Install_Count__c,
  user_count                                        AS Host_Users__c,
  updated_at                                        AS Last_Ping__c,
  usage_stats                                       AS Raw_Usage_Stats__c,
  version_link                                      AS Version_Link__c,
  gitlab_version                                    AS GitLab_version__c
FROM sfdc.account a INNER JOIN (
                                 SELECT
                                   company_name,
                                   the_clean_url                      AS company_domain,
                                   max(CASE WHEN company_type = 'Education'
                                     THEN 'Education'
                                       ELSE company_industry END) AS company_industry,
                                   max(CASE WHEN employees IS NOT NULL
                                     THEN employees :: INTEGER
                                       ELSE 0 END)                AS employees,
                                   max(hosts_count)               AS host_count,
                                   max(active_user_count)         AS user_count,
                                   max(updated_at)                AS updated_at,
                                   max(usage_stats)               AS usage_stats,
                                   max(version_link)              AS version_link,
                                   max(usage_data_gl_version)     AS gitlab_version
                                 FROM libre_agg
                                 GROUP BY company_name, company_domain
                               ) d ON d.company_name = a.name
WHERE a.isdeleted = FALSE AND ((d.host_count <> a.ce_instances__c OR a.ce_instances__c IS NULL) AND
                               (d.user_count <> a.active_ce_users__c OR a.active_ce_users__c IS NULL)) AND
      a.name NOT IN ('Microsoft', 'Amazon.com')
ORDER BY d.host_count DESC