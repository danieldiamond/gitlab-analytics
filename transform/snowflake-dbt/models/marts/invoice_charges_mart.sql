WITH charges_base AS (

    SELECT *
    FROM {{ ref('charges_base') }}

), fct_invoice_items_agg AS (

    SELECT *
    FROM {{ ref('fct_invoice_items_agg') }}

), charges_invoice_agg AS (

   SELECT
     charges_base.*,
     fct_invoice_items_agg.sku,
     fct_invoice_items_agg.service_start_date,
     fct_invoice_items_agg.service_end_date,
     fct_invoice_items_agg.charge_amount_sum,
     fct_invoice_items_agg.tax_amount_sum
   FROM charges_base
   INNER JOIN fct_invoice_items_agg
     ON charges_base.charge_id = fct_invoice_items_agg.charge_id
   WHERE charges_base.is_last_segment_version = TRUE

)

SELECT
  --date info
  subscription_start_month,
  subscription_end_month,
  effective_start_month,
  effective_end_month,

  --invoice info
  sku,
  service_start_date,
  service_end_date,
  charge_amount_sum,
  tax_amount_sum,

  --account info
  zuora_account_id,
  zuora_sold_to_country,
  zuora_account_name,
  zuora_account_number,
  crm_id,
  ultimate_parent_account_id,
  ultimate_parent_account_name,
  ultimate_parent_billing_country,

  --subscription info
  subscription_id,
  subscription_name_slugify,
  subscription_status,

  --charge info
  charge_id,
  product_name,
  rate_plan_name,
  product_category,
  delivery,
  service_type,
  charge_type,
  unit_of_measure,
  mrr,
  arr,
  quantity
FROM charges_invoice_agg
