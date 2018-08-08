{% macro retention_sfdc_parent_account_churn_history_cte_calc(the_month) %}


    current_subs_{{ the_month }} AS (
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
    ),

   acct_churn_{{ the_month }} AS (
    SELECT
        pa.account_id,
        pa.account_name,
        min(c.effective_start_date)             AS year_ago_start_date,
        max(c.effective_end_date)               AS year_ago_end_date,
        sum(c.mrr)                              AS year_ago_mrr,
        sum((c.mrr * (12) :: NUMERIC))          AS year_ago_arr,
        o.curr_start_date                       AS curr_start_date,
        o.curr_end_date                         AS curr_end_date,
        COALESCE(o.current_mrr, (0) :: NUMERIC) AS current_mrr,
        COALESCE(o.current_arr, (0) :: NUMERIC) AS current_arr
      FROM zuora_subs s
        JOIN zuora_accts a ON s.account_id = a.account_id :: TEXT
        JOIN zuora_rateplan r ON r.subscription_id :: TEXT = s.subscription_id
        JOIN zuora_rateplancharge c ON c.rate_plan_id :: TEXT = r.rate_plan_id :: TEXT
        JOIN zuora_accts ba ON a.account_id :: TEXT = ba.account_id
        JOIN sfdc_acct sa ON ba.crm_id = sa.account_id :: TEXT
        JOIN sfdc_acct pa ON sa.ultimate_parent_account_id = pa.account_id :: TEXT
        LEFT JOIN current_subs_{{ the_month }} o ON o.account_id :: TEXT = pa.account_id :: TEXT
      WHERE c.effective_start_date <= (date_trunc('month', current_date::DATE) - '{{ the_month }} month'::INTERVAL - '1 day'::INTERVAL - '1 year'::INTERVAL) AND
            (c.effective_end_date > (date_trunc('month', current_date::DATE) -'{{ the_month }} month'::INTERVAL - '1 day'::INTERVAL - '1 year'::INTERVAL)
                OR c.effective_end_date IS NULL)
      GROUP BY
        pa.account_id,
        pa.account_name,
        o.current_mrr,
        o.current_arr,
        COALESCE(o.amount, (0) :: NUMERIC),
        o.curr_start_date,
        o.curr_end_date
),


    trueups_{{ the_month }} AS (
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
        JOIN acct_churn_{{ the_month }} z ON z.account_id :: TEXT = pa.account_id :: TEXT
      WHERE r.rate_plan_name :: TEXT = 'Trueup' :: TEXT
      GROUP BY pa.account_id

    ),

    acct_churn_linked_{{ the_month }} AS (
        SELECT
            s.*,
            a.crm_id
        FROM acct_churn_{{ the_month }} s
        JOIN zuora_accts a ON s.account_id = a.account_id
    )

{% endmacro %}