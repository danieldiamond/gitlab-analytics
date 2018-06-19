-- this can't be an incremental model because of the day_range calculation
WITH zuora_invoice_base AS (

	SELECT * FROM {{ref('zuora_invoice')}}

), zuora_account AS (

	SELECT * FROM {{ref('zuora_account')}}

), zuora_contact AS (

	SELECT * FROM {{ref('zuora_contact')}}

), zuora_invoice AS(

	SELECT *, 
		DATE_PART('day', duedate - CURRENT_DATE) as 
	FROM zuora_invoice_base

)


SELECT zuora_account.entity,
       COALESCE(zuora_contact_bill.workemail,zuora_contact_sold.workemail) AS email,
       COALESCE(zuora_contact_sold.firstname,zuora_contact_bill.firstname) AS owner,
       zuora_account.account_name,
       zuora_account.account_number,
       zuora_account.currency,

       CASE
         WHEN days_until_due < 30 THEN '1: <30'
         WHEN days_until_due >= 30 AND days_until_due <= 60 THEN '2: 30-60'
         WHEN days_until_due >= 61 AND days_until_due <= 90 THEN '3: 61-90'
         WHEN days_until_due >= 91 THEN '4: >90'
         ELSE 'Unknown'
       END AS range_until_due,

       COALESCE(zuora_invoice.balance,0) AS balance,

       zuora_invoice.invoice_number AS invoice,
       zuora_invoice.due_date as due_date

FROM zuora_invoice
INNER JOIN zuora_account
  ON zuora_invoice.accountid = zuora_account.id
LEFT JOIN zuora.contact AS zuora_contact_bill
  ON zuora_contact_bill.id = zuora_account.billtocontact -- I don't really love this method, but it works. 
LEFT JOIN zuora.contact AS zuora_contact_sold
  ON zuora_contact_sold.id = zuora_account.soldtocontactid


WHERE (zuora_invoice.status = 'Posted')
AND   zuora_invoice.balance > 0
