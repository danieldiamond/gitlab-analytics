WITH zuora_subs AS (
    SELECT *
    FROM {{ ref('zuora_subscription') }}
    WHERE subscription_status <> ALL (ARRAY ['Draft' :: TEXT, 'Expired' :: TEXT])
),

    zuora_rateplan AS (
      SELECT *
      FROM {{ ref('zuora_rate_plan') }}
  ),

    zuora_rateplancharge AS (
      SELECT *
      FROM {{ ref('zuora_rate_plan_charge') }}
  ),

    acct_churn AS  (
      SELECT *
      FROM {{ ref('retention_zuora_sub_churn') }}
    )


      SELECT
        s.subscription_id,
        sum(
            CASE
            WHEN c.effective_start_date >= z.year_ago_start_date AND c.effective_start_date < z.year_ago_end_date
              THEN c.tcv
            ELSE 0 :: NUMERIC
            END) AS year_ago_trueup,
        sum(
            CASE
            WHEN c.effective_start_date >= z.curr_start_date AND c.effective_start_date < z.curr_end_date
              THEN c.tcv
            ELSE 0 :: NUMERIC
            END) AS current_trueup,
        sum(
            CASE
            WHEN c.effective_start_date >= (z.year_ago_start_date - '75 days' :: INTERVAL) AND
                 c.effective_start_date < (z.year_ago_end_date - '75 days' :: INTERVAL)
              THEN c.tcv
            ELSE 0 :: NUMERIC
            END) AS year_ago_trueup_new,
        sum(
            CASE
            WHEN c.effective_start_date >= (z.curr_start_date - '75 days' :: INTERVAL) AND c.effective_start_date < (z.curr_end_date - '75 days' :: INTERVAL)
              THEN c.tcv
            ELSE 0 :: NUMERIC
            END) AS current_trueup_new
      FROM zuora_subs s
        JOIN zuora_rateplan r ON r.subscription_id :: TEXT = s.subscription_id
        JOIN zuora_rateplancharge c ON c.rate_plan_id :: TEXT = r.rate_plan_id :: TEXT
        JOIN acct_churn z ON z.subscription_id :: TEXT = s.subscription_id:: TEXT
      WHERE r.rate_plan_name :: TEXT = 'Trueup' :: TEXT
      GROUP BY 1