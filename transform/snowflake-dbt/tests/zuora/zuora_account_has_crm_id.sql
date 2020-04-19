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
    AND zuora_account_id NOT IN (
                                '2c92a00d715350c501715933d41a25af' 
                                , '2c92a008715350de017163836e235b5f' 
                                , '2c92a00771643e8d01716ed232567f7a' 
                                , '2c92a0fd716449d2017167c8aaee760a' 
                                , '2c92a00771643e8c0171689eb51961e3' 
                                , '2c92a00d715350d2017159927fcd14e7' 
                                , '2c92a0077153417e01715f7a58210306' 
                                , '2c92a00e7153413301715b6082da7b70' 
                                , '2c92a00d715350c501715fdcb6df4f64' 
                                , '2c92a0ff71644a650171731b87410357' 
                                , '2c92a0077153417e01715ea7b8372acc' 
                                , '2c92a00771643e8c0171666668d27e4b' 
                                , '2c92a00c7153417801715b987ab24487' 
                                , '2c92a00e7153412e017159f9cbff075e' 
                                , '2c92a0087164499a0171700b724042a5' 
                                , '2c92a008715350df01715c6db7ab091e' 
                                , '2c92a0fd716449d20171731a93d47ead' 
                                , '2c92a00e71643e3b017165996c652c89' 
                                , '2c92a00c71643e750171694c65b8334f' 
                                , '2c92a0ff7153519a01715aa8d716207f' 
                                , '2c92a00c715341780171592c286721d1' 
                                , '2c92a0ff71536d080171585af52d6862' 
                                , '2c92a0fd716449d20171664eecd9596b' 
                                , '2c92a0fc715343d6017161a232df6853' 
                                , '2c92a00e71643e3b0171694bf91629fa' 
                                , '2c92a0fe715343d4017160fad8743b4a' 
                                , '2c92a008715350df01715dfdc9647b13' 
                                , '2c92a0fc715342ca0171636b69c9673c' 
                                , '2c92a0087164499a0171707f7f267491' 
                                , '2c92a00d716449900171651edccb15d4' 
                                , '2c92a00e71643e3b01716d215e833187' 
                                , '2c92a008715350de01716436d7d21d42' 
                                , '2c92a0fc7153434a01715b8957603ce8' 
                                , '2c92a0ff71644a65017165085f7c47fe' 
                                , '2c92a0fe71643f4a017164ca8b6e6257' 
                                , '2c92a0fe71643f4a01716654afb2641f' 
                                , '2c92a00c715341780171633e1d373b01' 
                                , '2c92a0fe7164404f017171904bb62f31' 
                                , '2c92a008715350dc0171637bae3643cb' 
                                , '2c92a00d71644990017169559a397982' 
                                , '2c92a0fe71643f4a01716935b45b640d' 
                                , '2c92a0fc715342ca01715a6e941752cf' 
                                , '2c92a0ff71644a650171690e71975570' 
                                , '2c92a00d715350d201715f298ff917ca' 
                                , '2c92a0fe715343d401715ec77fa6457d' 
                                , '2c92a008715350de017159a2a2a12069' 
                                , '2c92a0077153417e01715ea25b90177f' 
                                , '2c92a0ff71536d080171629d95347507' 
                                , '2c92a00c7153417801716232da406e6b' 
                            )
GROUP BY 1, 2, 3, 4
