-- This asserts that a Zuora subscription lineage only has a single ultimate parent account in Salesforce

WITH parentage AS (

  SELECT *
  FROM {{ ref('zuora_subscription_parentage_finish') }}

), zuora_subs AS (

 SELECT *
 FROM {{ ref('zuora_subscription_xf') }}

), zuora_accts AS (

 SELECT *
 FROM {{ ref('zuora_account') }}

), sfdc_accts AS (

 SELECT *
 FROM {{ ref('sfdc_accounts_xf') }}

), links AS (

SELECT ultimate_parent_sub,
      child_sub,
      a.account_number                 AS child_account,
      sa.account_name                  AS sfdc_account_name,
      sa.ultimate_parent_account_name  AS sfdc_ult_parent_name,
      a2.account_number                AS parent_account,
      sa2.account_name                 AS parent_sfdc_account_name,
      sa2.ultimate_parent_account_name AS parent_sfdc_ult_parent_name
FROM parentage s
LEFT JOIN zuora_subs xf ON s.child_sub = xf.subscription_name_slugify
LEFT JOIN zuora_subs xf2 ON s.ultimate_parent_sub = xf2.subscription_name_slugify
LEFT JOIN zuora_accts a ON xf.account_id = a.account_id
LEFT JOIN zuora_accts a2 ON xf2.account_id = a2.account_id
LEFT JOIN sfdc_accts sa ON a.crm_id = sa.account_id
LEFT JOIN sfdc_accts sa2 ON a2.crm_id = sa2.account_id

)

SELECT ultimate_parent_sub,
       count(DISTINCT child_sub)      AS child_sub_count,
       count(DISTINCT child_account)  AS child_account_count,
       count(DISTINCT parent_account) AS parent_account_count
FROM links
GROUP BY 1
HAVING parent_account_count > 1
ORDER BY 4 DESC