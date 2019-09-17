{{config({
    "schema": "staging"
  })
}}

WITH source AS (

  SELECT *
  FROM {{ source('zuora', 'refund') }}

), renamed AS (

  SELECT accountid                AS account_id,
         accountingcode           AS accounting_code,
         amount,
         billtocontactid          AS bill_to_contact_id,
         cancelledon              AS cancelled_on,
         comment,
         createdbyid              AS created_by_id,
         createddate              AS created_date,
         defaultpaymentmethodid   AS default_payment_method_id,
         deleted                  AS is_deleted,
         gateway,
         gatewayresponse          AS gateway_response,
         gatewayresponsecode      AS gateway_response_code,
         gatewaystate             AS gateway_state,
         id                       AS refund_id,
         methodtype               AS method_type,
         parentaccountid          AS parent_account_id,
         paymentmethodid          AS payment_method_id,
         paymentmethodsnapshotid  AS payment_method_snapshot_id,
         reasoncode               AS reason_code,
         referenceid              AS reference_id,
         refunddate               AS refund_date,
         refundnumber             AS refund_number,
         refundtransactiontime    AS refund_transaction_time,
         secondrefundreferenceid  AS second_refund_reference_id,
         softdescriptor           AS soft_descriptor,
         softdescriptorphone      AS soft_descriptor_phone,
         soldtocontactid          AS sold_to_contact_id,
         sourcetype               AS source_type,
         status                   AS refund_status,
         submittedon              AS submitted_on,
         transferredtoaccounting  AS transferred_to_accounting,
         type                     AS refund_type,
         updatedbyid              AS updated_by_id,
         updateddate              AS updated_date
   FROM source

)

SELECT *
FROM renamed
