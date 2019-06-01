WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'customer') }}

), renamed AS (

    SELECT
        internalid                          AS  customer_id,
        receivablesaccount['internalid']    AS  receivables_account_id,
        currency['internalid']              AS  currency_id,
        accessrole['internalid']            AS  access_role_id,
        entityid                            AS  entity_id,
        subsidiary['internalid']            AS  subsidiary_id,
        entitystatus['internalid']          AS  entity_status_id,

        stage['value']                      AS  stage,
        receivablesaccount['name']          AS  receivables_account_name,
        unbilledorders                      AS  unbilled_orders,
        depositbalance                      AS  deposit_balance,
        printtransactions                   AS  print_transactions,
        isperson                            AS  is_person,
        companyname                         AS  company_name,
        emailpreference['value']            AS  email_preference,
        emailtransactions                   AS email_transactions,
        accessrole['name']                  AS  access_role_name,
        alcoholrecipienttype['value']       AS  alcohol_recipient_type,
        overduebalance                      AS  overdue_balance,
        giveaccess                          AS  give_access,
        balance,
        weblead                             AS  web_lead,
        currency['name']                    AS  currency_name,
        isinactive                          AS  is_inactive,
        faxtransactions                     AS  fax_transactions,
        consoloverduebalance                AS  consol_overdue_balance,
        subsidiary['name']                  AS  subsidiary_name,
        consolunbilledorders                AS  consol_unbilled_orders,
        entitystatus['name']                AS  entity_status_name,
        creditholdoverride['value']         AS  credit_hold_override,
        aging,
        aging1,
        aging2,
        aging3,
        aging4,
        consolaging                         AS  consol_aging,
        consolaging1                        AS  consol_aging1,
        consolaging2                        AS  consol_aging2,
        consolaging3                        AS  consol_aging3,
        consolaging4                        AS  consol_aging4,

        datecreated                         AS  date_created,
        consolbalance                       AS  consol_balance,
        shipcomplete                        AS  ship_complete,
        consoldepositbalance                AS  consol_deposit_balance,
        currencylist                        AS  currency_list
    FROM source

)

SELECT *
FROM renamed


