with netsuite_transactions as (

	SELECT * FROM {{ref('netsuite_transactions')}}

), netsuite_expenses as (

	SELECT * FROM {{ref('netsuite_expenses')}}

)

SELECT netsuite_expenses.expense_id,
	netsuite_transactions.transaction_date,
    netsuite_expenses.transaction_id,
    netsuite_expenses.transaction_line,
    netsuite_expenses.account_id,
    netsuite_expenses.account_name,
    netsuite_expenses.amount,
    netsuite_expenses.department_id,
    netsuite_expenses.department_name,
    netsuite_expenses.category_id,
    netsuite_expenses.category_name,
    netsuite_expenses.memo,
    netsuite_expenses.customer_id,
    netsuite_expenses.customer_name as marketing_campaign
FROM netsuite_expenses
LEFT JOIN netsuite_transactions
ON netsuite_expenses.transaction_id = netsuite_transactions.transaction_id
WHERE netsuite_expenses.customer_id IS NOT NULL
AND LOWER(netsuite_expenses.department_name) LIKE '%marketing%'