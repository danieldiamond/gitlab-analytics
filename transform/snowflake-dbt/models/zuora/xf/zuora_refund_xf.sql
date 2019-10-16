WITH zuora_refund_base AS (

	SELECT *
	FROM {{ref('zuora_refund')}}

), zuora_account AS (

	SELECT * FROM {{ref('zuora_account')}}

)

SELECT za.sfdc_entity,
       za.account_name,
       za.account_number,
       za.currency,
       date_trunc('month',zr.refund_date)::DATE   AS refund_month,
       zr.refund_amount,
	   	 zr.comment,
	     zr.reason_code,
			 zr.source_type,
			 zr.refund_type,
	     zr.refund_status
FROM zuora_refund_base zr
LEFT JOIN zuora_account za
  ON zr.account_id = za.account_id
