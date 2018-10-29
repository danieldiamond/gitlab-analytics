with zuora_mrr_totals as (

    SELECT * FROM {{ref('zuora_mrr_totals')}}

), zuora_account as (

    SELECT * FROM {{ref('zuora_account')}}

), sfdc_accounts_xf as (

    SELECT * FROM {{ref('sfdc_accounts_xf')}}

), joined as (

    SELECT zuora_account.account_id,
           zuora_account.account_name,
           zuora_account.crm_id,
           sfdc_accounts_xf.ultimate_parent_account_id
    FROM zuora_mrr_totals
    LEFT JOIN zuora_account
        ON zuora_account.account_number = zuora_mrr_totals.account_number
    LEFT JOIN sfdc_accounts_xf
        ON sfdc_accounts_xf.account_id = zuora_account.crm_id

)

SELECT account_id,account_name
FROM joined
WHERE ultimate_parent_account_id IS NULL
GROUP BY 1, 2