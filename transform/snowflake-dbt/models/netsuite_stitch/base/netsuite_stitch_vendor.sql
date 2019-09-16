{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'vendor') }}

), renamed AS (

    SELECT
        addressbooklist                     AS address_book_list,
        balanceprimary                      AS balance_primary,
        category['interalId']::NUMBER       AS category_id,
        category['name']::STRING            AS category_name,
        comments,
        companyname                         AS company_name,
        currency['internalId']::NUMBER      AS currency_id,
        currency['name']::STRING            AS currency_name,
        currencylist                        AS currency_list,
        custentity1                         AS custom_entity_1,
        customfieldlist                     AS custom_field_list,
        datecreated                         AS date_created,
        defaultaddress                      AS default_address,
        email                               AS email,
        emailpreference['value']::STRING    AS email_preference,
        emailtransactions                   AS email_transactions,
        entityid                            AS entity_name,
        expenseaccount                      AS expense_account,
        faxtransactions                     AS fax_transactions,
        internalid                          AS vendor_id,
        is1099eligible                      AS is_1099_elgible,
        isinactive                          AS is_inactive,
        isjobresourcevend                   AS is_job_resource_vendor,
        isperson                            AS is_person,
        lastmodifieddate                    AS last_modified_date,
        legalname                           AS legal_name,
        payablesaccount                     AS payables_account,
        printtransactions                   AS print_transactions,
        subsidiary['internalId']::NUMBER    AS subsidiary_id,
        subsidiary['name']::STRING          AS subsidiary_name,
        taxidnum                            AS tax_id_number,
        terms,
        unbilledordersprimary               AS unbilled_orders_primary,
        externalid                          AS external_id,
        phone,
        salutation,
        title,
        middlename                          AS middle_name,
        custentity4                         AS custom_entity_4,
        lastname                            AS last_name,
        firstname                           AS first_name,
        url
    FROM source

)

SELECT *
FROM renamed


