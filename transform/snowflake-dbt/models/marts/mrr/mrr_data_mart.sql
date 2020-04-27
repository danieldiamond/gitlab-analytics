WITH fct_charges AS (

    SELECT *
    FROM {{ ref('fct_charges') }}

), dim_customers AS (

    SELECT *
    FROM {{ ref('dim_customers') }}

), dim_accounts AS (

    SELECT *
    FROM {{ ref('dim_accounts') }}

), dates AS (

   SELECT *
   FROM {{ ref('dim_dates') }}

), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

), charges_month_by_month AS (

   SELECT
    fct_charges.*,
    dim_dates.date_id,
    dim_dates.first_date_of_month AS mrr_month
    FROM fct_charges
    INNER JOIN dim_dates ON fct_charges.effective_start_month_id <= dim_dates.date_id
     AND fct_charges.effective_end_month_id > dim_dates.date_id
    WHERE dim_dates.day_of_month = 1
)

SELECT
  charges_month_by_month.mrr_month,
  dim_account.account_id,
  COALESCE(dim_customers.merged_to_account_id, dim_customers.crm_id) AS crm_id,
  dim_accounts.account_name,
  dim_customers.ultimate_parent_account_id,
  dim_customers.ultimate_parent_account_name,
  dim_customers.ultimate_parent_billing_country,
  dim_accounts.sold_to_country,
  dim_subscriptions.subscription_name_slugify,
  dim_subscriptions.subscription_status,
  charges_month_by_month.mrr,
  charges_month_by_month.quantity
  FROM charges_month_by_month
  INNER JOIN dim_subscriptions
    ON dim_subscriptions.subscription_id = charges_month_by_month.subscription_id
  INNER JOIN dim_customers
    ON dim_customers.crm_id = dim_subscriptions.crm_id
  INNER JOIN dim_accounts
    ON charges_month_by_month.account_id = dim_accounts.account_id
