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
  )


  SELECT
    s_1.subscription_id,
    s_1.account_id,
    min(s_1.term_start_date)     AS curr_start_date,
    max(s_1.term_end_date)       AS curr_end_date,
    sum(c_1.mrr)                 AS current_mrr,
    sum(c_1.mrr * 12 :: NUMERIC) AS current_arr,
    sum(c_1.tcv)                 AS amount
  FROM zuora_subs s_1
    JOIN zuora_rateplan r_1         ON r_1.subscription_id :: TEXT = s_1.subscription_id
    JOIN zuora_rateplancharge c_1   ON c_1.rate_plan_id :: TEXT = r_1.rate_plan_id :: TEXT
  WHERE c_1.effective_start_date <= current_date
        AND (c_1.effective_end_date > current_date OR c_1.effective_end_date IS NULL)
  GROUP BY 1,2