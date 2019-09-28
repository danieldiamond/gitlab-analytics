WITH posting_account_activity AS (

     SELECT *
     FROM {{ ref('netsuite_posting_account_activity_xf') }}

), accounts AS (

     SELECT *
     FROM {{ ref('netsuite_account_xf') }}

), accounting_periods AS (

     SELECT *
     FROM {{ ref('netsuite_accounting_periods') }}

), subsidiaries AS (

     SELECT *
     FROM {{ ref('netsuite_subsidiaries') }}

), consolidated_exchange_rates AS (

     SELECT *
     FROM {{ ref('netsuite_consolidated_exchange_rates') }}

), departments AS (

     SELECT *
     FROM {{ ref('netsuite_departments_xf') }}

), budget AS (

     SELECT *
     FROM {{ ref('netsuite_budget') }}

), income_statement AS (

    SELECT b.account_name,
           b.unique_account_number,
           b.account_number,
           b.ultimate_account_number,
           c.accounting_period_starting_date::DATE                           AS accounting_period_starting_date,
           c.accounting_period_name,
           c.accounting_period_full_name,
           f.department_name,
           COALESCE(f.ultimate_department_name, 'zNeed Accounting Reclass')  AS parent_department_name,
           SUM(CASE WHEN a.subsidiary_id = 1 THEN -a.actual_amount
                    ELSE (-a.actual_amount * e.average_rate)
               END)                                                          AS actual_amount,
           SUM(CASE WHEN b.account_number BETWEEN '4000' AND '4010' THEN 0
                    WHEN g.budget_amount IS NULL THEN 0
                    ELSE -g.budget_amount
               END)                                                          AS budget_amount
    FROM posting_account_activity a
    LEFT JOIN accounts b
      ON a.account_id = b.account_id
    LEFT JOIN accounting_periods c
      ON a.accounting_period_id = c.accounting_period_id
    LEFT JOIN subsidiaries d
      ON a.subsidiary_id = d.subsidiary_id
    LEFT JOIN consolidated_exchange_rates e
      ON a.accounting_period_id = e.accounting_period_id
      AND a.subsidiary_id = e.from_subsidiary_id
    LEFT JOIN departments f
      ON a.department_id = f.department_id
    LEFT JOIN budget g
      ON a.accounting_period_id = g.accounting_period_id
      AND a.account_id = g.account_id
      AND a.department_id = g.department_id
    WHERE c.fiscal_calendar_id = 2
      AND e.to_subsidiary_id = 1
      AND b.account_number between '4000' and '6999'
      AND g.subsidiary_id = 1
    {{ dbt_utils.group_by(n=9) }}

)

SELECT *
FROM income_statement
