{% macro zuora_sfdc_f_churn_history(n) %}

WITH acct_churn AS (
    SELECT *
    FROM {{ ref('zuora_sfdc_parent_account_churn') }}
),

    trueups AS (
        SELECT *
        FROM {{ ref('zuora_sfdc_parent_account_trueup') }}

    )

{% for the_month in range(0, n + 1) %}
        SELECT
          date_part('year', current_date - '{{ the_month }} month'::INTERVAL) || '/M' ||
          CASE
            WHEN date_part('month', current_date - '{{ the_month }} month'::INTERVAL) < 10
                THEN '0' || date_part('month', current_date - '{{ the_month }} month'::INTERVAL) :: TEXT
            ELSE date_part('month', current_date - '{{ the_month }} month'::INTERVAL) :: TEXT
                END                            AS period,
          s.account_id,
          s.year_ago_start_date,
          s.year_ago_end_date,
          s.year_ago_mrr,
          s.year_ago_arr,
          COALESCE(t.year_ago_trueup, 0 :: NUMERIC)                                                                               AS year_ago_trueup,
          round(COALESCE(t.year_ago_trueup, 0 :: NUMERIC) + s.year_ago_arr, 2)                                                    AS year_ago_total,
          s.curr_start_date,
          s.curr_end_date,
          s.current_mrr,
          s.current_arr,
          COALESCE(t.current_trueup, 0 :: NUMERIC)                                                                                AS current_trueup,
          round(COALESCE(t.current_trueup, 0 :: NUMERIC) + s.current_arr, 2)                                                      AS current_total,
          COALESCE(t.current_trueup, 0 :: NUMERIC) + s.current_arr - (COALESCE(t.year_ago_trueup, 0 :: NUMERIC) + s.year_ago_arr) AS change
        FROM acct_churn s
          LEFT JOIN trueups t ON t.account_id :: TEXT = s.account_id :: TEXT

    {% if  the_month != n %}

            UNION ALL

        {% else %}

        {%- endif -%}

     {% endfor %}

{% endmacro %}