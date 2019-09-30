WITH posting_account_activity AS (

     SELECT *
     FROM {{ ref('netsuite_posting_account_activity_xf') }}

), accounts AS (

     SELECT *
     FROM {{ ref('netsuite_accounts_xf') }}

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

), income_statement_actual AS (

    SELECT accounts.account_name,
           accounts.unique_account_number,
           accounts.account_number,
           accounts.ultimate_account_number,
           accounting_periods.accounting_period_starting_date::DATE                    AS accounting_period_starting_date,
           accounting_periods.accounting_period_name,
           accounting_periods.accounting_period_full_name,
           departments.department_name,
           COALESCE(departments.ultimate_department_name, 'zNeed Accounting Reclass')  AS ultimate_department_name,
           SUM(CASE WHEN posting_account_activity.subsidiary_id = 1 THEN posting_account_activity.actual_amount
                    ELSE (posting_account_activity.actual_amount * consolidated_exchange_rates.average_rate)
               END)                                                                    AS actual_amount
    FROM posting_account_activity
    LEFT JOIN accounts
      ON posting_account_activity.account_id = accounts.account_id
    LEFT JOIN accounting_periods
      ON posting_account_activity.accounting_period_id = accounting_periods.accounting_period_id
    LEFT JOIN subsidiaries
      ON posting_account_activity.subsidiary_id = subsidiaries.subsidiary_id
    LEFT JOIN consolidated_exchange_rates
      ON posting_account_activity.accounting_period_id = consolidated_exchange_rates.accounting_period_id
      AND posting_account_activity.subsidiary_id = consolidated_exchange_rates.from_subsidiary_id
    LEFT JOIN departments
      ON posting_account_activity.department_id = departments.department_id
    WHERE accounting_periods.fiscal_calendar_id = 2
      AND consolidated_exchange_rates.to_subsidiary_id = 1
      AND accounts.account_number BETWEEN '4000' AND '6999'
    {{ dbt_utils.group_by(n=9) }}

), income_statement_grouping AS (

    SELECT account_number || ' - ' || account_name      AS account_full_name,
           account_name,
           unique_account_number,
           account_number,
           ultimate_account_number,
           accounting_period_starting_date,
           accounting_period_name,
           accounting_period_full_name,
           department_name,
           ultimate_department_name,
           CASE WHEN account_number BETWEEN '4000' AND '4999' THEN '1-Income'
                WHEN account_number BETWEEN '5000' AND '5999' THEN '2-Cost of Sales'
                WHEN account_number BETWEEN '6000' AND '6999' THEN '3-Expense'
           END                                          AS income_statement_grouping,
           CASE WHEN account_number BETWEEN '4000' AND '4999' THEN -actual_amount
                ELSE actual_amount
           END                                          AS actual_amount
           FROM income_statement_actual
           WHERE account_number NOT IN ('4005','5077','5079','5080')

)

SELECT *
FROM income_statement_grouping
ORDER BY 6,11,1
