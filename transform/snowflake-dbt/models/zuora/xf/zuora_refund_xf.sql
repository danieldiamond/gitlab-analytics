WITH zuora_refund_base AS (

	SELECT *
	FROM {{ref('zuora_refund')}}

), zuora_account AS (

	SELECT * FROM {{ref('zuora_account')}}

)

SELECT zuora_account.sfdc_entity,
       zuora_account.account_name,
       zuora_account.account_number,
       zuora_account.currency,
       date_trunc('month',refund_date)::DATE   AS refund_month,
       amount                                  AS refund_amount,
			 comment,
			 reason_code,
			 is_deleted,
			 zuora_refund_base.refund_status
FROM zuora_refund_base
LEFT JOIN zuora_account
  ON zuora_refund_base.account_id = zuora_account.account_id
