{% macro retention_zuora_sub_churn_calc(n) %}

-- This replaces the

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

    current_subs AS (
      SELECT *
      FROM {{ ref('retention_zuora_sub_current_subscriptions') }}
  )


    {% for the_month in range(0, n + 1) %}
      SELECT
        s.subscription_id,
        s.subscription_name,
        min(c.effective_start_date)             AS year_ago_start_date,
        max(c.effective_end_date)               AS year_ago_end_date,
        sum(c.mrr)                              AS year_ago_mrr,
        sum((c.mrr * (12) :: NUMERIC))          AS year_ago_arr,
        o.curr_start_date                       AS curr_start_date,
        o.curr_end_date                         AS curr_end_date,
        COALESCE(o.current_mrr, (0) :: NUMERIC) AS current_mrr,
        COALESCE(o.current_arr, (0) :: NUMERIC) AS current_arr
      FROM zuora_subs s
        JOIN zuora_rateplan r ON r.subscription_id :: TEXT = s.subscription_id
        JOIN zuora_rateplancharge c ON c.rate_plan_id :: TEXT = r.rate_plan_id :: TEXT
        LEFT JOIN current_subs o ON o.subscription_id :: TEXT = s.subscription_id:: TEXT
      WHERE c.effective_start_date <= (date_trunc('month', current_date::DATE) - '{{ the_month }} month'::INTERVAL - '1 day'::INTERVAL - '1 year'::INTERVAL) AND
            (c.effective_end_date > (date_trunc('month', current_date::DATE) -'{{ the_month }} month'::INTERVAL - '1 day'::INTERVAL - '1 year'::INTERVAL)
                OR c.effective_end_date IS NULL)
      GROUP BY
        s.subscription_id,
        s.subscription_name,
        o.current_mrr,
        o.current_arr,
        COALESCE(o.amount, (0) :: NUMERIC),
        o.curr_start_date,
        o.curr_end_date

      {% if  the_month != n %}

            UNION ALL

        {% else %}

        {%- endif -%}

     {% endfor %}

{% endmacro %}

