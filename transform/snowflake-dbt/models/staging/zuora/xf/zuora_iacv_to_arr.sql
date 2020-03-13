WITH sfdc_opportunity_xf AS (

    SELECT
      TO_NUMBER(amount, 38, 2) AS opportunity_amount,
      close_date::DATE         AS close_date,
      invoice_number,
      net_incremental_acv,
      opportunity_id,
      sales_type
    FROM {{ ref('sfdc_opportunity_xf') }}
    WHERE stage_name = 'Closed Won'
      AND invoice_number IS NOT NULL  

), zuora_invoice_charges AS (

    SELECT
      effective_start_date,
      effective_end_date,
      invoice_amount_without_tax AS invoice_amount,
      invoice_date,
      invoice_item_charge_amount AS item_amount,
      invoice_number,
      is_last_segment_version,
      mrr, 
      {{ product_category('rate_plan_name') }},
      {{ delivery('product_category') }},  
      rate_plan_charge_name      AS charge_name,  
      rate_plan_charge_number    AS charge_number, 
      rate_plan_charge_segment   AS charge_segment, 
      subscription_name_slugify
    FROM {{ ref('zuora_invoice_charges') }}

), filtered_charges AS (

    SELECT *
    FROM zuora_invoice_charges
    WHERE effective_end_date > invoice_date
        AND mrr > 0

), true_mrr_periods AS (

    SELECT
      charge_number,
      charge_segment,
      effective_start_date AS true_effective_start_date,
      effective_end_date   AS true_effective_end_date
    FROM zuora_invoice_charges
    WHERE is_last_segment_version  

), aggregate_subscription AS (

    SELECT
      subscription_name_slugify,
      invoice_number,
      invoice_date,
      COUNT(DISTINCT subscription_name_slugify) OVER (PARTITION BY invoice_number)  AS invoiced_subscriptions,
      TO_NUMBER(MAX(invoice_amount), 38, 2)                                         AS invoice_amount,
      TO_NUMBER(SUM(item_amount), 38, 2)                                            AS subscription_amount,
      TO_NUMBER(SUM(IFF(charge_name = '1,000 CI Minutes', item_amount, 0)), 38, 2)  AS ci_minutes_amount,
      TO_NUMBER(SUM(IFF(charge_name = 'Trueup', item_amount, 0)), 38, 2)            AS trueup_amount
    FROM zuora_invoice_charges
    GROUP BY 1,2,3  

), joined AS (

    SELECT
      -- keys
      aggregate_subscription.subscription_name_slugify,
      aggregate_subscription.invoice_number,
      filtered_charges.charge_number,
      sfdc_opportunity_xf.opportunity_id,

      -- dates
      aggregate_subscription.invoice_date,
      filtered_charges.effective_start_date AS booked_effective_start_date,
      filtered_charges.effective_end_date   AS booked_effective_end_date,
      true_mrr_periods.true_effective_start_date,
      true_mrr_periods.true_effective_end_date,
      sfdc_opportunity_xf.close_date,

      -- invoice info
      aggregate_subscription.invoiced_subscriptions,
      aggregate_subscription.invoice_amount,
      aggregate_subscription.subscription_amount,
      aggregate_subscription.ci_minutes_amount,
      aggregate_subscription.trueup_amount,
      filtered_charges.item_amount,
      sfdc_opportunity_xf.opportunity_amount,

      -- charge info
      filtered_charges.mrr,
      sfdc_opportunity_xf.net_incremental_acv,

      -- metadata
      filtered_charges.charge_name,
      filtered_charges.charge_segment,
      filtered_charges.delivery,
      filtered_charges.product_category,
      sfdc_opportunity_xf.sales_type,
      MAX(is_last_segment_version) AS is_last_segment_version
    FROM aggregate_subscription
    INNER JOIN filtered_charges
      ON aggregate_subscription.subscription_name_slugify = filtered_charges.subscription_name_slugify
      AND aggregate_subscription.invoice_number = filtered_charges.invoice_number
    INNER JOIN true_mrr_periods
      ON filtered_charges.charge_number = true_mrr_periods.charge_number
      AND filtered_charges.charge_segment = true_mrr_periods.charge_segment  
    LEFT JOIN sfdc_opportunity_xf
      ON aggregate_subscription.invoice_number = sfdc_opportunity_xf.invoice_number
      AND aggregate_subscription.subscription_amount = sfdc_opportunity_xf.opportunity_amount
    {{ dbt_utils.group_by(n=24)}}      

)

SELECT *
FROM joined