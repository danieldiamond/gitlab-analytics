WITH zuora_accts AS (

    SELECT *
    FROM {{ ref('zuora_account') }}

),

    zuora_subs AS (

    SELECT *
    FROM {{ ref('zuora_subscription_xf') }}

),

    zuora_rp AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan') }}

),

    zuora_rpc AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge') }}

),

    date_table AS (

     SELECT
      last_day_of_month
     FROM {{ ref('date_details') }}

)

SELECT md5(zuora_rpc.rate_plan_charge_id||zuora_subs.subscription_id) as unique_key,
      zuora_accts.account_number,
      zuora_subs.subscription_name,
      zuora_subs.subscription_name_slugify,
      zuora_subs.subscription_slug_for_counting,
      zuora_rp.rate_plan_name,
      zuora_rpc.rate_plan_charge_name,
      zuora_rpc.mrr,
      date_trunc('month', zuora_subs.subscription_start_date) :: DATE                         AS sub_start_month,
      (date_trunc('month', zuora_subs.subscription_end_date) - '1 month' :: INTERVAL) :: DATE AS sub_end_month,
      date_trunc('month', zuora_rpc.effective_start_date) :: DATE                           AS effective_start_month,
      (date_trunc('month', zuora_rpc.effective_end_date) - '1 month' :: INTERVAL) :: DATE   AS effective_end_month,
      date_part('month',age(zuora_rpc.effective_end_date, zuora_rpc.effective_start_date)) +
          (date_part('year',age(zuora_rpc.effective_end_date, zuora_rpc.effective_start_date)) * 12) AS month_interval,
      zuora_rpc.effective_start_date,
      zuora_rpc.effective_end_date,
      zuora_subs.cohort_month,
      zuora_subs.cohort_quarter,
      zuora_rpc.unit_of_measure,
      zuora_rpc.quantity
    FROM zuora_accts
      LEFT JOIN zuora_subs ON zuora_accts.account_id = zuora_subs.account_id
      LEFT JOIN zuora_rp ON zuora_rp.subscription_id = zuora_subs.subscription_id
      LEFT JOIN zuora_rpc ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
      WHERE zuora_rpc.mrr > 0
--      -- The following lines remove rate plan charges that are less than 1 month but not if they're on or span the last day of the month
        AND NOT
                (
                    date_trunc('month', zuora_rpc.effective_end_date) :: DATE =
                    date_trunc('month', zuora_rpc.effective_start_date) :: DATE
                      AND
                    (
                        zuora_rpc.effective_start_date NOT IN (SELECT last_day_of_month FROM date_table)
                          OR
                        zuora_rpc.effective_end_date NOT IN (SELECT last_day_of_month FROM date_table)

                    )
                )
