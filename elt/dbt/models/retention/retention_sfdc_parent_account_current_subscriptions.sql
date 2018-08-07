WITH zuora_subs AS (
    SELECT *
    FROM {{ ref('zuora_subscription') }}
    WHERE subscription_status <> ALL (ARRAY ['Draft' :: TEXT, 'Expired' :: TEXT])
),

    zuora_accts AS (
      SELECT *
      FROM {{ ref('zuora_account') }}
  ),

    zuora_rateplan AS (
      SELECT *
      FROM {{ ref('zuora_rate_plan') }}
  ),

    zuora_rateplancharge AS (
      SELECT *
      FROM {{ ref('zuora_rate_plan_charge') }}
  ),

    sfdc_acct AS (
      SELECT *
      FROM {{ ref('sfdc_account') }}
  )


  SELECT
    pa_1.account_id,
    min(s_1.term_start_date)     AS curr_start_date,
    max(s_1.term_end_date)       AS curr_end_date,
    sum(c_1.mrr)                 AS current_mrr,
    sum(c_1.mrr * 12 :: NUMERIC) AS current_arr,
    sum(c_1.tcv)                 AS amount
  FROM zuora_subs s_1
    JOIN zuora_accts a_1            ON s_1.account_id = a_1.account_id :: TEXT
    JOIN zuora_rateplan r_1         ON r_1.subscription_id :: TEXT = s_1.subscription_id
    JOIN zuora_rateplancharge c_1   ON c_1.rate_plan_id :: TEXT = r_1.rate_plan_id :: TEXT
    JOIN zuora_accts ba_1           ON a_1.account_id :: TEXT = ba_1.account_id
    JOIN sfdc_acct sa_1             ON ba_1.crm_id = sa_1.account_id :: TEXT
    JOIN sfdc_acct pa_1             ON sa_1.ultimate_parent_account_id = pa_1.account_id :: TEXT
  WHERE c_1.effective_start_date <= current_date
        AND (c_1.effective_end_date > current_date OR c_1.effective_end_date IS NULL)
  GROUP BY pa_1.account_id