with zuora_mrr_totals as (

    SELECT * FROM {{ref('zuora_mrr_totals')}}

), zuora_account as (

    SELECT * FROM {{ref('zuora_account')}}

), sfdc_accounts_xf as (

    SELECT * FROM {{ref('sfdc_accounts_xf')}}

), sfdc_deleted_accounts as (

    SELECT * FROM {{ref('sfdc_deleted_accounts')}}

), joined as (

    SELECT zuora_account.account_id     as zuora_account_id,
           zuora_account.account_number as zuora_account_number,
           zuora_account.account_name,
           zuora_account.crm_id,
           sfdc_accounts_xf.account_id  as sfdc_account_id,
           sfdc_accounts_xf.ultimate_parent_account_id
    FROM zuora_mrr_totals
    LEFT JOIN zuora_account
        ON zuora_account.account_number = zuora_mrr_totals.account_number
    LEFT JOIN sfdc_accounts_xf
        ON sfdc_accounts_xf.account_id = zuora_account.crm_id
    WHERE zuora_account.updated_date::date < current_date

), final as (

    SELECT zuora_account_id,
            account_name,
            crm_id,
            coalesce(joined.sfdc_account_id, sfdc_master_record_id) as sfdc_account_id
    FROM joined
    LEFT JOIN sfdc_deleted_accounts
    ON crm_id = sfdc_deleted_accounts.sfdc_account_id
)

SELECT *
FROM final
WHERE sfdc_account_id IS NULL
GROUP BY 1, 2, 3, 4
