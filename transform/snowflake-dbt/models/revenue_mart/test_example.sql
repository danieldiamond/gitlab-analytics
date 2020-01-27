WITH zuora_accts AS (

	SELECT *
  FROM {{ source('zuora', 'account') }}

), zuora_subscription AS (

	SELECT *
  FROM {{ source('zuora', 'subscription') }}

), zuora_rp AS (

  SELECT *
  FROM {{ source('zuora', 'rate_plan') }}

), zuora_rpc AS (

	SELECT *
  FROM {{ source('zuora', 'rate_plan_charge') }}

), zuora_contact AS (

  SELECT *
  FROM {{ source('zuora', 'contact') }}

), zuora_rev_sch AS (

  SELECT *
  FROM {{ source('zuora', 'revenue_schedule_item') }}

), zuora_acct_period AS (

  SELECT *
  FROM {{ source('zuora', 'accounting_period') }}

), zuora_product AS (

  SELECT *
  FROM {{ source('zuora', 'product') }}

), date_table AS (

  SELECT *
  FROM {{ ref('date_details') }}
  WHERE day_of_month = 1

), list AS (

	SELECT DISTINCT
    zuora_acct_period.startdate::DATE AS accounting_period,
    zuora_product.name                AS product_name,
    zuora_rpc.name                    AS rate_plan_charge_name
  FROM zuora_product
  CROSS JOIN zuora_acct_period
  CROSS JOIN zuora_rpc
  WHERE zuora_acct_period.startdate::DATE <= current_date
  ORDER BY 1 DESC,2,3

), base_mrr AS (

  SELECT
    zuora_rpc.id AS rate_plan_charge_id,

    -- account info
    zuora_accts.name AS account_name,
    zuora_accts.accountnumber AS account_number,
    zuora_contact.country,
    zuora_accts.currency,

    -- rate_plan info
    --zuora_rp.delivery,
    --zuora_rp.product_category,
    zuora_rp.name AS rate_plan_name,

    zuora_rpc.name AS rate_plan_charge_name,
    zuora_rpc.chargenumber AS rate_plan_charge_number,
    zuora_rpc.tcv,
    zuora_rpc.uom AS unit_of_measure,

    zuora_product.name AS product_name,

    date_trunc('month', zuora_rpc.effectivestartdate::DATE) AS effective_start_month,
    date_trunc('month', zuora_rpc.effectiveenddate::DATE) AS effective_end_month,

    zuora_rpc.effectivestartdate AS effective_start_date,
    zuora_rpc.effectiveenddate AS effective_end_date,
    zuora_rpc.quantity AS quantity,
    zuora_rpc.mrr AS mrr
  FROM zuora_accts
  INNER JOIN zuora_subscription
    ON zuora_accts.id = zuora_subscription.accountid
  INNER JOIN zuora_rp
    ON zuora_rp.subscriptionid = zuora_subscription.id
  INNER JOIN zuora_rpc
    ON zuora_rpc.rateplanid = zuora_rp.id
  LEFT JOIN zuora_contact
    ON COALESCE(zuora_accts.soldtocontactid ,zuora_accts.billtocontactid) = zuora_contact.id
  LEFT JOIN zuora_product
    ON zuora_product.id = zuora_rpc.productid
  WHERE zuora_subscription.status NOT IN ('Draft','Expired')

), month_base_mrr AS (

  SELECT
    --country,
    --account_number,
    --rate_plan_name,
    --product_category,
    --delivery,
    date_actual AS accounting_period,
    product_name,
		rate_plan_charge_name,
    SUM(mrr) AS mrr,
    SUM(quantity) AS quantity
  FROM base_mrr
  LEFT JOIN date_table
    ON base_mrr.effective_start_date <= date_table.date_actual
    AND (base_mrr.effective_end_date > date_table.date_actual OR base_mrr.effective_end_date IS NULL)
  {{ dbt_utils.group_by(n=3) }}

), gaap_revenue AS (

  SELECT
    zuora_acct_period.startdate::DATE  AS accounting_period,
    zuora_product.name                 AS product_name,
		zuora_rpc.name						         AS rate_plan_charge_name,
    SUM(zuora_rev_sch.amount)          AS revenue_amt
  FROM zuora_rev_sch
  INNER JOIN zuora_rpc
    ON zuora_rev_sch.rateplanchargeid = zuora_rpc.id
  INNER JOIN zuora_acct_period
    ON zuora_acct_period.id = zuora_rev_sch.accountingperiodid
  LEFT JOIN zuora_product
    ON zuora_product.id = zuora_rev_sch.productid
  {{ dbt_utils.group_by(n=3) }}

), current_mrr AS (

   SELECT
     SUM(zuora_rpc.mrr) AS current_mrr
   FROM zuora_accts
   INNER JOIN zuora_subscription
     ON zuora_accts.id = zuora_subscription.accountid
   INNER JOIN zuora_rp
     ON zuora_rp.subscriptionid = zuora_subscription.id
   INNER JOIN zuora_rpc
     ON zuora_rpc.rateplanid = zuora_rp.id
   WHERE zuora_subscription.status NOT IN ('Draft','Expired')
     AND effectivestartdate <= current_date
     AND (effectiveenddate > current_date OR effectiveenddate IS NULL)

), combined AS (

   SELECT
     list.accounting_period,
     list.product_name,
	   list.rate_plan_charge_name,
     MAX(current_mrr)   AS current_mrr,
     SUM(mrr)           AS mrr,
     SUM(revenue_amt)   AS revenue_amt
   FROM list
   LEFT JOIN gaap_revenue
     ON list.accounting_period = gaap_revenue.accounting_period
     AND list.product_name = gaap_revenue.product_name
     AND list.rate_plan_charge_name = gaap_revenue.rate_plan_charge_name
   LEFT JOIN month_base_mrr
     ON list.accounting_period = month_base_mrr.accounting_period
	   AND list.product_name = month_base_mrr.product_name
	   AND list.rate_plan_charge_name = month_base_mrr.rate_plan_charge_name
   LEFT JOIN current_mrr
   {{ dbt_utils.group_by(n=3) }}

)

SELECT
  accounting_period,
  product_name,
  rate_plan_charge_name,
  current_mrr,
  CASE
    WHEN rate_plan_charge_name = '#movingtogitlab'
      THEN revenue_amt
    ELSE mrr
  END  											AS mrr,
  revenue_amt
FROM combined
WHERE revenue_amt IS NOT NULL
  OR mrr IS NOT NULL
