with libre_agg as (
  SELECT * FROM {{ ref('libre_agg_hosts') }}
)

SELECT
  sf.id                                                        AS Account__c,
  coalesce(sf.website, 'www.' || lah.the_clean_url)            AS host_domain__c,
  sf.name                                                      AS Host_Name__c,
  CASE WHEN sf.type = 'Prospect'
    THEN 'Prospect - CE User'
  ELSE sf.type END                                             AS Host_Type__c,
  sf.billingstate                                              AS Billing_State__c,
  sf.billingcountry                                            AS Billing_Country__c,
  coalesce(sf.Industry, lah.company_industry)                  AS Host_Industry__c,
  coalesce(sf.numberofemployees,
           CASE WHEN lah.employees = 0
             THEN NULL
           ELSE lah.employees END)                             AS Host_Employees__c,
  lah.ping_type                                                AS Host_Ping_Type__c,
  lah.max_active_user_count                                    AS Host_Users__c,
  lah.ping_date                                                AS Last_Ping__c,
  lah.ping_json_data :: TEXT                                   AS Raw_Usage_Stats__c,
  'https://version.gitlab.com/servers/' || lah.host_id :: TEXT AS Version_Link__c,
  lah.gitlab_version                                           AS GitLab_version__c,
  lah.gitlab_edition                                           AS GitLab_edition__c,
  lah.license_ids                                              AS License_Ids__c,
  lah.mattermost_enabled                                       AS Mattermost_Enabled__c,
  lah.source                                                   AS Host_Data_Source__c
FROM sfdc.account sf
  INNER JOIN libre_agg AS lah
    ON lah.company_name = sf.name
WHERE sf.isdeleted = FALSE
      AND (lah.max_active_user_count != sf.active_ce_users__c OR sf.active_ce_users__c IS NULL)
      AND sf.name NOT IN ('Microsoft', 'Amazon.com')

UNION

SELECT
  sf.id                                                        AS Account__c,
  coalesce(sf.website, 'www.' || lah.the_clean_url)            AS host_domain__c,
  sf.name                                                      AS Host_Name__c,
  CASE WHEN sf.type = 'Prospect'
    THEN 'Prospect - CE User'
  ELSE sf.type END                                             AS Host_Type__c,
  sf.billingstate                                              AS Billing_State__c,
  sf.billingcountry                                            AS Billing_Country__c,
  coalesce(sf.Industry, lah.company_industry)                  AS Host_Industry__c,
  coalesce(sf.numberofemployees,
           CASE WHEN lah.employees = 0
             THEN NULL
           ELSE lah.employees END)                             AS Host_Employees__c,
  lah.ping_type                                                AS Host_Ping_Type__c,
  lah.max_active_user_count                                    AS Host_Users__c,
  lah.ping_date                                                AS Last_Ping__c,
  lah.ping_json_data :: TEXT                                   AS Raw_Usage_Stats__c,
  'https://version.gitlab.com/servers/' || lah.host_id :: TEXT AS Version_Link__c,
  lah.gitlab_version                                           AS GitLab_version__c,
  lah.gitlab_edition                                           AS GitLab_edition__c,
  lah.license_ids                                              AS License_Ids__c,
  lah.mattermost_enabled                                       AS Mattermost_Enabled__c,
  lah.source                                                   AS Host_Data_Source__c
FROM sfdc.account sf
  INNER JOIN libre_agg AS lah
    ON lah.the_clean_url = regexp_replace(sf.website, '^(http(s)?\://)?www\.', '')
WHERE sf.isdeleted = FALSE
      AND (lah.max_active_user_count != sf.active_ce_users__c OR sf.active_ce_users__c IS NULL)
      AND sf.name NOT IN ('Microsoft', 'Amazon.com')