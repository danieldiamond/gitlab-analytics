with libre_agg as (
  SELECT * FROM {{ ref('libre_agg_hosts') }}
)

SELECT
  sf.id                                                        AS Account__c,
  lah.clean_full_domain                                        AS Name,
  lah.raw_domain                                               AS Original_Hostname__c,
  lah.active_user_count                                        AS Host_Users__c,
  lah.ping_date                                                AS Last_Ping__c,
  lah.ping_usage_data :: TEXT                                  AS Raw_Usage_Stats__c,
  lah.ping_version_data :: TEXT                                AS Raw_Version_Stats__c,
  'https://version.gitlab.com/servers/' || lah.host_id :: TEXT AS Version_Link__c,
  lah.gitlab_version                                           AS GitLab_Version__c,
  lah.gitlab_edition                                           AS GitLab_edition__c,
  lah.license_id                                               AS License_Ids__c,
  lah.mattermost_enabled                                       AS Mattermost_Enabled__c,
  lah.source                                                   AS Host_Data_Source__c
FROM sfdc_sandbox.account sf
  INNER JOIN libre_agg AS lah
    ON lah.company_name = sf.name
WHERE sf.isdeleted = FALSE
      AND (lah.active_user_count != sf.active_ce_users__c OR sf.active_ce_users__c IS NULL)
      AND sf.name NOT IN ('Microsoft', 'Amazon.com')

UNION

SELECT
  sf.id                                                        AS Account__c,
  lah.clean_full_domain                                        AS Name,
  lah.raw_domain                                               AS Original_Hostname__c,
  lah.active_user_count                                        AS Host_Users__c,
  lah.ping_date                                                AS Last_Ping__c,
  lah.ping_usage_data :: TEXT                                  AS Raw_Usage_Stats__c,
  lah.ping_version_data :: TEXT                                AS Raw_Version_Stats__c,
  'https://version.gitlab.com/servers/' || lah.host_id :: TEXT AS Version_Link__c,
  lah.gitlab_version                                           AS GitLab_Version__c,
  lah.gitlab_edition                                           AS GitLab_edition__c,
  lah.license_id                                               AS License_Ids__c,
  lah.mattermost_enabled                                       AS Mattermost_Enabled__c,
  lah.source                                                   AS Host_Data_Source__c
FROM sfdc_sandbox.account sf
  INNER JOIN libre_agg AS lah
    ON lah.the_clean_url = regexp_replace(sf.website, '^(http(s)?\://)?www\.', '')
WHERE sf.isdeleted = FALSE
      AND (lah.active_user_count != sf.active_ce_users__c OR sf.active_ce_users__c IS NULL)
      AND sf.name NOT IN ('Microsoft', 'Amazon.com')