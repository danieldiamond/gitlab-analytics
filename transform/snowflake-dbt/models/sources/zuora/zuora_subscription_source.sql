-- depends_on: {{ ref('zuora_excluded_accounts') }}

WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'subscription') }}

), renamed AS (

    SELECT
      id                                          AS subscription_id,
      subscriptionversionamendmentid              AS amendment_id,
      name                                        AS subscription_name,
        {{zuora_slugify("name")}}                 AS subscription_name_slugify,
      --keys
      accountid                                   AS account_id,
      creatoraccountid                            AS creator_account_id,
      creatorinvoiceownerid                       AS creator_invoice_owner_id,
      invoiceownerid                              AS invoice_owner_id,
      nullif(opportunityid__c, '')                AS sfdc_opportunity_id,
      nullif(originalid, '')                      AS original_id,
      nullif(previoussubscriptionid, '')          AS previous_subscription_id,
      nullif(recurlyid__c, '')                    AS sfdc_recurly_id,
      cpqbundlejsonid__qt                         AS cpq_bundle_json_id,

      -- info
      status                                      AS subscription_status,
      autorenew                                   AS auto_renew,
      version                                     AS version,
      termtype                                    AS term_type,
      notes                                       AS notes,
      isinvoiceseparate                           AS is_invoice_separate,
      currentterm                                 AS current_term,
      currenttermperiodtype                       AS current_term_period_type,
      clickthrougheularequired__c                 AS sfdc_click_through_eula_required,
      endcustomerdetails__c                       AS sfdc_end_customer_details,

      --key_dates
      cancelleddate                               AS cancelled_date,
      contractacceptancedate                      AS contract_acceptance_date,
      contracteffectivedate                       AS contract_effective_date,
      initialterm                                 AS initial_term,
      initialtermperiodtype                       AS initial_term_period_type,
      termenddate::DATE                           AS term_end_date,
      termstartdate::DATE                         AS term_start_date,
      subscriptionenddate::DATE                   AS subscription_end_date,
      subscriptionstartdate::DATE                 AS subscription_start_date,
      serviceactivationdate                       AS service_activiation_date,
      opportunityclosedate__qt                    AS opportunity_close_date,
      originalcreateddate                         AS original_created_date,

      --foreign synced info
      opportunityname__qt                         AS opportunity_name,
      purchase_order__c                           AS sfdc_purchase_order,
      --purchaseorder__c                            AS sfdc_purchase_order_,
      quotebusinesstype__qt                       AS quote_business_type,
      quotenumber__qt                             AS quote_number,
      quotetype__qt                               AS quote_type,

      --renewal info
      renewalsetting                              AS renewal_setting,
      renewal_subscription__c__c                  AS zuora_renewal_subscription_name,

      split(nullif({{zuora_slugify("renewal_subscription__c__c")}}, ''), '|')
                                                  AS zuora_renewal_subscription_name_slugify,
      renewalterm                                 AS renewal_term,
      renewaltermperiodtype                       AS renewal_term_period_type,
      exclude_from_renewal_report__c__c           AS exclude_from_renewal_report,

      --metadata
      updatedbyid                                 AS updated_by_id,
      updateddate                                 AS updated_date,
      createdbyid                                 AS created_by_id,
      createddate                                 AS created_date,
      deleted                                     AS is_deleted,
      excludefromanalysis__c                      AS exclude_from_analysis

    FROM source

)

SELECT *
FROM renamed
