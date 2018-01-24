with account as (
		select * from {{ ref('account') }}
)

SELECT row_number() OVER (
                          ORDER BY sfdc_account_id) AS id,
       sfdc_account_id,
       name,
       industry,
       type 
FROM account 