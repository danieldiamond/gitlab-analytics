{{config({
    "materialized": "table",
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'account') }}

), renamed AS (

    SELECT
      id                                       AS account_id,
      name                                     AS account_name,
      -- keys
      account_id_18__c                         AS account_id_18,
      masterrecordid                           AS master_record_id,
      ownerid                                  AS owner_id,
      parentid                                 AS parent_id,
      primary_contact_id__c                    AS primary_contact_id,
      recordtypeid                             AS record_type_id,
      ultimate_parent_account_id__c            AS utimate_parent_id,
      partner_vat_tax_id__c                    AS partner_vat_tax_id,

      -- key people GL side
      entity__c                                AS gitlab_entity,
      federal_account__c                       AS federal_account,
      gitlab_com_user__c                       AS gitlab_com_user,
      account_manager__c                       AS account_manager,
      account_owner_calc__c                    AS account_owner,
      account_owner_team__c                    AS account_owner_team,
      business_development_rep__c              AS business_development_rep,
      dedicated_service_engineer__c            AS dedicated_service_engineer,
      sdr__c                                   AS sales_development_rep,
      sdr_account_team__c                      AS sales_development_rep_team,
      solutions_architect__c                   AS solutions_architect,
      technical_account_manager_lu__c          AS technical_account_manager_id,

    --   info
      {{target.schema}}_staging.id15to18(substring(regexp_replace(ultimate_parent_account__c,
                     '_HL_ENCODED_/|<a\\s+href="/', ''), 0, 15))                 
                                               AS ultimate_parent_account_id,
      type                                     AS account_type,
      industry,
      product_category__c                      AS product_category,
      account_tier__c                          AS account_tier,
      customer_since__c :: DATE                AS customer_since_date,
      carr_total__c                            AS carr_total,
      next_renewal_date__c                     AS next_renewal_date,
      license_utilization__c                   AS license_utilization,
      products_purchased__c                    AS products_purchased,
      region__c                                AS account_region,
      total_account_value__c                   AS total_account_value,
      sub_region__c                            AS account_sub_region,
      sales_segmentation_new__c                AS account_segment,
      support_level__c                         AS support_level,
      support_level_numeric__c                 AS support_level_numeric,
      named_account__c                         AS named_account,
      billingcountry                           AS billing_country, 
      billingpostalcode                        AS billing_postal_code, 
      billinglongitude                         AS billing_longitude, 
      billinglatitude                          AS billing_latitude,

      --present state info
      health__c                                AS health_score,
      health_score_reasons__c                  AS health_score_explained,


      -- opportunity metrics
      count_of_active_subscription_charges__c  AS count_active_subscription_charges,
      count_of_active_subscriptions__c         AS count_active_subscriptions,
      count_of_billing_accounts__c             AS count_billing_accounts,
      license_user_count__c                    AS count_licensed_users,
      count_of_new_business_won_opps__c        AS count_of_new_business_won_opportunities,
      count_of_open_renewal_opportunities__c   AS count_open_renewal_opportunities,
      count_of_opportunities__c                AS count_opportunities,
      count_of_products_purchased__c           AS count_products_purchased,
      count_of_won_opportunities__c            AS count_won_opportunities,
      concurrent_ee_subscriptions__c           AS count_concurrent_ee_subscriptions,
      ce_instances__c                          AS count_ce_instances,
      active_ce_users__c                       AS count_active_ce_users,
      number_of_open_opportunities__c          AS count_open_opportunities,
      using_ce__c                              AS count_using_ce,


      -- metadata
      createdbyid                              AS created_by_id,
      createddate                              AS created_date,
      isdeleted                                AS is_deleted,
      lastmodifiedbyid                         AS last_modified_by_id,
      lastmodifieddate                         AS last_modified_date,
      lastactivitydate                         AS last_activity_date,
      lastreferenceddate                       AS last_referenced_date,
      lastvieweddate                           AS last_viewed_date,
      convert_timezone('America/Los_Angeles',convert_timezone('UTC',current_timestamp())) AS _last_dbt_run,
      systemmodstamp


    FROM source
    WHERE id IS NOT NULL
    AND isdeleted = FALSE

)

SELECT *
FROM renamed
