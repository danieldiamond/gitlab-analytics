WITH fct_charges AS (

    SELECT *
    FROM {{ ref('fct_charges') }}

), dim_customers AS (

    SELECT *
    FROM {{ ref('dim_customers') }}

), dim_accounts AS (

    SELECT *
    FROM {{ ref('dim_accounts') }}

), dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

), dim_products AS (

    SELECT *
    FROM {{ ref('dim_products') }}

)

SELECT
  --date info
  fct_charges.effective_start_date_id,
  fct_charges.effective_end_date_id,
  fct_charges.effective_start_month,
  fct_charges.effective_end_month,
  dim_subscriptions.subscription_start_month,
  dim_subscriptions.subscription_end_month,

  --account info
  dim_accounts.account_id                                              AS zuora_account_id,
  dim_accounts.sold_to_country                                         AS zuora_sold_to_country,
  dim_accounts.account_name                                            AS zuora_account_name,
  dim_accounts.account_number                                          AS zuora_account_number,
  COALESCE(dim_customers.merged_to_account_id, dim_customers.crm_id)   AS crm_id,
  dim_customers.ultimate_parent_account_id,
  dim_customers.ultimate_parent_account_name,
  dim_customers.ultimate_parent_billing_country,

  --subscription info
  dim_subscriptions.subscription_id,
  dim_subscriptions.subscription_name_slugify,
  dim_subscriptions.subscription_status,

  --charge info
  fct_charges.charge_id,
  fct_charges.is_last_segment_version,
  dim_products.product_name,
  fct_charges.rate_plan_name,
  fct_charges.product_category,
  fct_charges.delivery,
  fct_charges.service_type,
  fct_charges.charge_type,
  fct_charges.unit_of_measure,
  fct_charges.mrr,
  fct_charges.mrr*12                                                    AS arr,
  fct_charges.quantity
FROM fct_charges
INNER JOIN dim_subscriptions
  ON fct_charges.subscription_id = dim_subscriptions.subscription_id
INNER JOIN dim_products
  ON fct_charges.product_id = dim_products.product_id
INNER JOIN dim_customers
  ON dim_subscriptions.crm_id = dim_customers.crm_id
INNER JOIN dim_accounts
  ON fct_charges.account_id = dim_accounts.account_id
