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

    SELECT accounts.account_number || ' - ' || accounts.account_name                   AS account_full_name,
           accounts.account_name,
           accounts.unique_account_number,
           accounts.account_number,
           accounts.ultimate_account_number,
           accounting_periods.accounting_period_starting_date::DATE                    AS accounting_period_starting_date,
           accounting_periods.accounting_period_name,
           accounting_periods.accounting_period_full_name,
           departments.department_name,
           budget_category.budget_name,
           COALESCE(departments.parent_department_name, 'zNeed Accounting Reclass')    AS parent_department_name,
           CASE WHEN account_number BETWEEN '4000' AND '4999' THEN '1-Income'
                WHEN account_number BETWEEN '5000' AND '5999' THEN '2-Cost of Sales'
                WHEN account_number BETWEEN '6000' AND '6999' THEN '3-Expense'
           END                                                                         AS income_statement_grouping,
           SUM(CASE WHEN accounts.account_number BETWEEN '4000' AND '4010' THEN 0
                    WHEN budget.budget_amount IS NULL THEN 0
                    ELSE budget.budget_amount
               END)                                                                    AS budget_amount
    FROM budget
    LEFT JOIN budget_category
      ON budget.category_id = budget_category.budget_category_id
    LEFT JOIN accounts
      ON budget.account_id = accounts.account_id
    LEFT JOIN accounting_periods
      ON budget.accounting_period_id = accounting_periods.accounting_period_id
    LEFT JOIN departments
      ON budget.department_id = departments.department_id
    WHERE accounting_periods.fiscal_calendar_id = 2
      AND accounts.account_number between '4000' and '6999'
    {{ dbt_utils.group_by(n=12) }}

)

SELECT *
FROM income_statement_budget
ORDER BY 6,12,1
