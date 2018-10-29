WITH zuora_accts AS (

    SELECT *
    FROM {{ ref('zuora_account') }}

),

    zuora_subs AS (

    SELECT *
    FROM {{ ref('zuora_subscription') }}

),

    zuora_rp AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan') }}

),

    zuora_rpc AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge') }}
    WHERE rate_plan_charge_name ~ 'Trueup'

),

    zuora_i AS (

    SELECT *
    FROM {{ ref('zuora_invoice') }}
    WHERE status = 'Posted' -- posted is important!

),

    date_table AS (

     SELECT
      last_day_of_month
     FROM {{ ref('date_details') }}

),

    zuora_ii AS (

    SELECT
      *,
      date_trunc('month', service_start_date) :: DATE   AS trueup_month -- use current month
    FROM {{ ref('zuora_invoice_item') }}
    WHERE charge_name ~ 'Trueup'

),

     sub_months AS (

    SELECT
      account_number,
      cohort_month,
      cohort_quarter,
      subscription_name,
      subscription_name_slugify,
      subscription_slug_for_counting
    FROM {{ ref('zuora_base_mrr') }}
    GROUP BY 1, 2, 3, 4, 5, 6

),


    trueups as (

      SELECT s.subscription_name,
             s.subscription_name_slugify,
             ii.trueup_month,
             ii.invoice_item_id,
             ii.charge_name,
             ii.service_start_date,
             ii.charge_amount -- Only need the charge_amount, for trueups, quantity* unit price always matches to 2 decimals
      FROM zuora_ii ii
             INNER JOIN zuora_i i ON i.invoice_id = ii.invoice_id
             LEFT JOIN zuora_rpc rpc ON rpc.rate_plan_charge_id = ii.rate_plan_charge_id
             LEFT JOIN zuora_rp rp ON rpc.rate_plan_id = rp.rate_plan_id
             LEFT JOIN zuora_subs s ON rp.subscription_id = s.subscription_id
  )

SELECT
  mr.account_number,
  mr.subscription_name,
  mr.subscription_name_slugify,
  mr.subscription_slug_for_counting,
  mr.cohort_month,
  mr.cohort_quarter,
  t.trueup_month,
  t.charge_name,
  t.service_start_date,
  t.charge_amount,
  t.charge_amount/12 as mrr
FROM trueups t
LEFT JOIN sub_months mr ON t.subscription_name_slugify = mr.subscription_name_slugify
    WHERE mr.cohort_month IS NOT NULL
