WITH budget AS (

     SELECT *
     FROM {{ ref('netsuite_budget') }}

), budget_category AS (

     SELECT *
     FROM {{ ref('netsuite_budget_category') }}

), accounts AS (

     SELECT *
     FROM {{ ref('netsuite_accounts_xf') }}

), accounting_periods AS (

     SELECT *
     FROM {{ ref('netsuite_accounting_periods') }}

), subsidiaries AS (

     SELECT *
     FROM {{ ref('netsuite_subsidiaries') }}

), departments AS (

     SELECT *
     FROM {{ ref('netsuite_departments_xf') }}

), income_statement_budget AS (

    SELECT b.account_number || ' - ' || b.account_name                      AS account_full_name,
           b.account_name,
           b.unique_account_number,
           b.account_number,
           b.ultimate_account_number,
           c.accounting_period_starting_date::DATE                           AS accounting_period_starting_date,
           c.accounting_period_name,
           c.accounting_period_full_name,
           f.department_name,
           g.budget_name,
           COALESCE(f.ultimate_department_name, 'zNeed Accounting Reclass')  AS ultimate_department_name,
           SUM(CASE WHEN b.account_number BETWEEN '4000' AND '4010' THEN 0
                    WHEN a.budget_amount IS NULL THEN 0
                    ELSE a.budget_amount
               END)                                                          AS budget_amount
    FROM budget a
    LEFT JOIN budget_category g
      ON a.category_id = g.budget_category_id
    LEFT JOIN accounts b
      ON a.account_id = b.account_id
    LEFT JOIN accounting_periods c
      ON a.accounting_period_id = c.accounting_period_id
    LEFT JOIN departments f
      ON a.department_id = f.department_id
    WHERE c.fiscal_calendar_id = 2
      AND b.account_number between '4000' and '6999'
    {{ dbt_utils.group_by(n=11) }}

)

SELECT *
FROM income_statement_budget
