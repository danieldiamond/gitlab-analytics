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

), date_table AS (

  SELECT *
  FROM {{ ref('date_details') }}
  WHERE day_of_month = 1

), base_mrr AS (

  SELECT
    zuora_rpc.id AS rate_plan_charge_id,

    -- account info
    zuora_accts.name AS account_name,
    zuora_accts.accountnumber AS account_number,
    zuora_contact.country,
    zuora_accts.currency,

    -- subscription info
    --zuora_subscriptions_xf.subscription_name,
    --zuora_subscriptions_xf.subscription_name_slugify,
    --zuora_subscriptions_xf.subscription_start_date,

    --subscription_lineage info
    --zuora_subscriptions_xf.exclude_from_renewal_report,
    --zuora_subscriptions_xf.lineage,
    --zuora_subscriptions_xf.oldest_subscription_in_cohort,
    --zuora_subscriptions_xf.subscription_status,

    --cohort info
    --zuora_subscriptions_xf.cohort_month,
    --zuora_subscriptions_xf.cohort_quarter

    -- rate_plan info
    --zuora_rp.delivery,
    --zuora_rp.product_category,
    zuora_rp.name AS rate_plan_name,

    zuora_rpc.name AS rate_plan_charge_name,
    zuora_rpc.chargenumber AS rate_plan_charge_number,
    zuora_rpc.tcv,
    zuora_rpc.uom AS unit_of_measure,


    --date_trunc('month', zuora_subscriptions_xf.subscription_start_date :: date) AS sub_start_month,
    --date_trunc('month', dateadd('month', -1, zuora_subscriptions_xf.subscription_end_date :: DATE)) AS sub_end_month,
    date_trunc('month', zuora_rpc.effectivestartdate::DATE) AS effective_start_month,
    date_trunc('month', zuora_rpc.effectiveenddate::DATE) AS effective_end_month,
    --datediff(month, zuora_rpc.effectivestartdate::DATE,zuora_rpc.effectiveenddate::DATE) AS month_interval,
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
  WHERE zuora_subscription.status NOT IN ('Draft','Expired')

), month_base_mrr AS (

  SELECT
    --country,
    --account_number,
    --subscription_name,
    --subscription_name_slugify,
    --oldest_subscription_in_cohort,
    --lineage,
    --rate_plan_name,
    --product_category,
    --delivery,
    --rate_plan_charge_name,
    date_actual AS mrr_month,
    --sub_start_month,
    --sub_end_month,
    --effective_start_month,
    --effective_end_month,
    --effective_start_date,
    --effective_end_date,
    --cohort_month,
    --cohort_quarter,
    --unit_of_measure,
    SUM(mrr) AS mrr,
    SUM(quantity) AS quantity
  FROM base_mrr
  LEFT JOIN date_table
    ON base_mrr.effective_start_date <= date_table.date_actual
    AND (base_mrr.effective_end_date > date_table.date_actual OR base_mrr.effective_end_date IS NULL)
  {{ dbt_utils.group_by(n=1) }}

), gaap_revenue AS (

  SELECT
    zuora_acct_period.startdate::DATE  AS accounting_period,
    SUM(zuora_rev_sch.amount)          AS revenue_amt
  FROM zuora_rev_sch
  INNER JOIN zuora_rpc
    ON zuora_rev_sch.rateplanchargeid = zuora_rpc.id
  INNER JOIN zuora_acct_period
    ON zuora_acct_period.id = zuora_rev_sch.accountingperiodid
  {{ dbt_utils.group_by(n=1) }}

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

)

SELECT
  mrr_month,
  MAX(current_mrr)   AS current_mrr,
  SUM(mrr)           AS mrr,
  SUM(revenue_amt)   AS revenue_amt
FROM month_base_mrr
LEFT JOIN gaap_revenue
  ON month_base_mrr.mrr_month = gaap_revenue.accounting_period
LEFT JOIN current_mrr
{{ dbt_utils.group_by(n=1) }}
