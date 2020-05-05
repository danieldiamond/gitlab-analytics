WITH fct_charges AS (

    SELECT *
    FROM {{ ref('fct_charges') }}

), fct_invoice_items_agg AS (
    SELECT *
    FROM {{ ref('fct_invoice_items_agg')}}

), dim_customers AS (

    SELECT *
    FROM {{ ref('dim_customers') }}

), dim_dates AS (

   SELECT *
   FROM {{ ref('dim_dates') }}

), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

), dim_products AS (

    SELECT *
    FROM {{ ref('dim_products') }}

), charges_month_by_month AS (

   SELECT
    fct_charges.*,
    dim_dates.date_id,
    dateadd('month', -1, dim_dates.date_actual)  AS reporting_month
    FROM fct_charges
    INNER JOIN dim_dates
      ON fct_charges.effective_start_date_id <= dim_dates.date_id
        AND (fct_charges.effective_end_date_id > dim_dates.date_id OR fct_charges.effective_end_date_id IS NULL)
        AND dim_dates.day_of_month=1
)

SELECT
  charges_month_by_month.reporting_month,
  dim_customers.zuora_account_id,
  dim_customers.zuora_sold_to_country,
  dim_customers.zuora_account_name,
  dim_customers.zuora_account_number,
  COALESCE(dim_customers.merged_to_account_id, dim_customers.crm_id)   AS crm_id,
  dim_customers.ultimate_parent_account_id,
  dim_customers.ultimate_parent_account_name,
  dim_customers.ultimate_parent_billing_country,
  dim_subscriptions.subscription_id,
  dim_subscriptions.subscription_name_slugify,
  dim_subscriptions.subscription_status,
  dim_subscriptions.subscription_start_date,
  dim_subscriptions.subscription_end_date,
  charges_month_by_month.effective_start_month,
  charges_month_by_month.effective_end_month,
  dim_products.product_name,
  charges_month_by_month.rate_plan_charge_name,
  charges_month_by_month.rate_plan_name,
  charges_month_by_month.product_category,
  charges_month_by_month.delivery,
  charges_month_by_month.service_type,
  charges_month_by_month.unit_of_measure,
  charges_month_by_month.mrr,
  charges_month_by_month.mrr*12 as ARR,
  charges_month_by_month.quantity
  FROM charges_month_by_month
  INNER JOIN dim_subscriptions
    ON dim_subscriptions.subscription_id = charges_month_by_month.subscription_id
  INNER JOIN dim_products
    ON charges_month_by_month.product_id = dim_products.product_id
  INNER JOIN dim_customers
    ON dim_customers.crm_id = dim_subscriptions.crm_id
  INNER JOIN fct_invoice_items_agg
    ON charges_month_by_month.charge_id = fct_invoice_items_agg.charge_id
  WHERE charges_month_by_month.is_last_segment_version = TRUE
    AND mrr IS NOT NULL
