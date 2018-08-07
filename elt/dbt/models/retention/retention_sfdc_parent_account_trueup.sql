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
  ),

    acct_churn AS  (
      SELECT *
      FROM {{ ref('retention_sfdc_parent_account_churn') }}
    )


      SELECT
        pa.account_id,
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
        JOIN zuora_accts a ON s.account_id = a.account_id :: TEXT
        JOIN zuora_rateplan r ON r.subscription_id :: TEXT = s.subscription_id
        JOIN zuora_rateplancharge c ON c.rate_plan_id :: TEXT = r.rate_plan_id :: TEXT
        JOIN zuora_accts ba ON a.account_id :: TEXT = ba.account_id
        JOIN sfdc_acct sa ON ba.crm_id = sa.account_id :: TEXT
        JOIN sfdc_acct pa ON sa.ultimate_parent_account_id = pa.account_id :: TEXT
        JOIN acct_churn z ON z.account_id :: TEXT = pa.account_id :: TEXT
      WHERE r.rate_plan_name :: TEXT = 'Trueup' :: TEXT
      GROUP BY pa.account_id