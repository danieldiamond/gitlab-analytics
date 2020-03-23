WITH self_managed_charges AS (
  
  SELECT *
  FROM {{ ref('customers_db_charges_xf') }} AS charges
  WHERE charges.delivery = 'Self-Managed' --AND is_purchased_through_subscription_portal = FALSE
      AND charges.subscription_start_date = charges.subscription_version_term_start_date
  
)

, zuora_base_mrr AS (
  
  SELECT *
  FROM {{ ref('zuora_base_mrr') }}

)

, zuora_account AS (
  
  SELECT *
  FROM {{ ref('zuora_account') }}
  
)

, sfdc_accounts_xf AS (
  
  SELECT *
  FROM {{ ref('sfdc_accounts_xf') }}

)

, sfdc_lead_xf AS (
  
  SELECT *
  FROM {{ ref('sfdc_lead_xf') }}
  WHERE lead_source IN (
                        'GitLab.com',
                        'Trial - Enterprise',
                        'GitLab Subscription Portal',
                        'Trial - GitLab.com',
                        'CE Download'
                        )
  
)

, self_managed_joined AS (
  
  SELECT DISTINCT
  
    self_managed_charges.subscription_name_slugify, 
    self_managed_charges.mrr,
    self_managed_charges.month_interval, 
    self_managed_charges.subscription_start_date, 
    sfdc_accounts_xf.created_date  AS sfdc_account_created_date, 
    FIRST_VALUE(lead_source) OVER (PARTITION BY self_managed_charges.subscription_name_slugify ORDER BY sfdc_lead_xf.created_date ASC) AS first_lead_source,
    MIN(sfdc_lead_xf.created_date) OVER (PARTITION BY self_managed_charges.subscription_name_slugify) AS first_sfdc_lead_created_date
  
  FROM self_managed_charges
  LEFT JOIN zuora_base_mrr 
    ON self_managed_charges.rate_plan_charge_id = zuora_base_mrr.rate_plan_charge_id
  LEFT JOIN zuora_account AS zuora_account
    ON zuora_base_mrr.account_number = zuora_account.account_number
  LEFT JOIN sfdc_accounts_xf
  ON zuora_account.crm_id = sfdc_accounts_xf.account_id
  INNER JOIN sfdc_lead_xf ON sfdc_accounts_xf.account_id = sfdc_lead_xf.converted_account_id
 
)

, self_managed_joined_with_datediff AS (
  
    SELECT 
      *,
      DATEDIFF('day', 
               COALESCE(first_sfdc_lead_created_date, sfdc_account_created_date)
               , subscription_start_date) AS days_between_first_sfdc_record_and_subscription
    FROM self_managed_joined
  
)

, self_managed_calculation AS (
  
    SELECT
      DATE_TRUNC('month', subscription_start_date)::DATE AS subscription_month,
      subscription_name_slugify,
      CASE 
       WHEN SUM(month_interval) <= 12
        THEN SUM(mrr * month_interval)
       ELSE SUM(mrr * month_interval) * 12/ SUM(month_interval)
      END AS arr
    FROM self_managed_joined_with_datediff
    WHERE TRUE AND days_between_first_sfdc_record_and_subscription > 0
      AND subscription_month < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1,2

)


SELECT *
FROM self_managed_calculation
