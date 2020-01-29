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

), gaap_revenue AS (

	SELECT
	  zuora_acct_period.startdate::DATE		AS accounting_period,

	  -- account info
	  zuora_accts.name						AS account_name,
	  zuora_accts.accountnumber				AS account_number,
	  zuora_accts.crmid						AS crm_id,
	  zuora_contact.country,
	  zuora_accts.currency,

	  --rate_plan info
	  zuora_rpc.name						AS rate_plan_charge_name,
	  zuora_rpc.chargenumber				AS rate_plan_charge_number,
	  zuora_rpc.tcv,
	  zuora_rpc.uom							AS unit_of_measure,
	  zuora_rpc.quantity					AS quantity,
	  zuora_rpc.mrr							AS mrr,
	  zuora_product.name					AS product_name,
	  SUM(zuora_rev_sch.amount)				AS revenue_amt
	FROM zuora_rev_sch
	LEFT JOIN zuora_accts
	  ON zuora_rev_sch.accountid = zuora_accts.id
	LEFT JOIN zuora_contact
	  ON COALESCE(zuora_accts.soldtocontactid ,zuora_accts.billtocontactid) = zuora_contact.id
	INNER JOIN zuora_rpc
	  ON zuora_rev_sch.rateplanchargeid = zuora_rpc.id
	INNER JOIN zuora_acct_period
	  ON zuora_acct_period.id = zuora_rev_sch.accountingperiodid
	LEFT JOIN zuora_product
	  ON zuora_product.id = zuora_rev_sch.productid
	{{ dbt_utils.group_by(n=13) }}

)

SELECT *
FROM gaap_revenue
