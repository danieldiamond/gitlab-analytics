WITH date_table AS (

    SELECT *
    FROM {{ ref('date_details') }}
    WHERE day_of_month = 1

), gaap_revenue AS (

    SELECT *
    FROM {{ ref('gaap_revenue_base') }}

), zuora_accts AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

), zuora_acct_period AS (

    SELECT *
    FROM {{ ref('zuora_accounting_period_source') }}

), zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact_source') }}

), zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}

), zuora_rp AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), zuora_rpc AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

), base_mrr AS (

    SELECT
      --keys
      zuora_rpc.rate_plan_charge_id,

      --account info
      zuora_accts.account_name,
      zuora_accts.account_number,
      zuora_accts.crm_id,
      zuora_contact.country,
      zuora_accts.currency,

      --rate_plan info
      zuora_rp.rate_plan_name,
      zuora_rpc.rate_plan_charge_name,
      zuora_rpc.rate_plan_charge_number,
      zuora_rpc.tcv,
      zuora_rpc.unit_of_measure,
      zuora_rpc.quantity,
      zuora_rpc.mrr,
      zuora_product.product_name,

      --date info
      date_trunc('month', zuora_rpc.effective_start_date::DATE)            AS effective_start_month,
      date_trunc('month', zuora_rpc.effective_end_date::DATE)            AS effective_end_month,
      zuora_rpc.effective_start_date,
      zuora_rpc.effective_end_date
    FROM zuora_accts
    INNER JOIN zuora_subscription
      ON zuora_accts.account_id = zuora_subscription.account_id
    INNER JOIN zuora_rp
      ON zuora_rp.subscription_id = zuora_subscription.subscription_id
    INNER JOIN zuora_rpc
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_accts.sold_to_contact_id ,zuora_accts.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN zuora_product
      ON zuora_product.product_id = zuora_rpc.product_id
    WHERE zuora_subscription.subscription_status NOT IN ('Draft','Expired')

), month_base_mrr AS (

    SELECT
      date_actual                                   AS mrr_month,
      account_number,
      crm_id,
      account_name,
      country,
      {{ product_category('rate_plan_name') }},
      {{ delivery('product_category')}},
      product_name,
      rate_plan_name,
      rate_plan_charge_name,
      SUM(mrr)                                      AS mrr,
      SUM(quantity)                                 AS quantity
    FROM base_mrr
    LEFT JOIN date_table
      ON base_mrr.effective_start_date <= date_table.date_actual
      AND (base_mrr.effective_end_date > date_table.date_actual OR base_mrr.effective_end_date IS NULL)
    {{ dbt_utils.group_by(n=10) }}

), movingtogitlab AS (

    SELECT
      accounting_period                     AS mrr_month,
      rate_plan_charge_name,
      SUM(revenue_amt)                         AS revenue_amt
    FROM gaap_revenue
    WHERE rate_plan_charge_name = '#movingtogitlab'
    GROUP BY 1,2

), current_mrr AS (

    SELECT
      SUM(zuora_rpc.mrr) AS current_mrr
    FROM zuora_accts
    INNER JOIN zuora_subscription
      ON zuora_accts.account_id = zuora_subscription.account_id
    INNER JOIN zuora_rp
      ON zuora_rp.subscription_id = zuora_subscription.subscription_id
    INNER JOIN zuora_rpc
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
    WHERE zuora_subscription.subscription_status NOT IN ('Draft','Expired')
      AND effective_start_date <= current_date
      AND (effective_end_date > current_date OR effective_end_date IS NULL)

)

SELECT
  dateadd('month',-1,month_base_mrr.mrr_month)                          AS mrr_month,
  account_number,
  crm_id,
  account_name,
  country,
  product_category,
  delivery,
  product_name,
  rate_plan_name,
  month_base_mrr.rate_plan_charge_name,
  MAX(current_mrr)                                                        AS current_mrr,
  SUM(CASE
        WHEN month_base_mrr.rate_plan_charge_name = '#movingtogitlab'
          THEN revenue_amt
        ELSE mrr
       END)                                                               AS mrr,
  SUM((CASE
         WHEN month_base_mrr.rate_plan_charge_name = '#movingtogitlab'
           THEN revenue_amt
         ELSE mrr
       END)*12)                                                            AS arr
FROM month_base_mrr
LEFT JOIN movingtogitlab
  ON month_base_mrr.mrr_month = movingtogitlab.mrr_month
  AND month_base_mrr.rate_plan_charge_name = movingtogitlab.rate_plan_charge_name
LEFT JOIN current_mrr
{{ dbt_utils.group_by(n=10) }}
