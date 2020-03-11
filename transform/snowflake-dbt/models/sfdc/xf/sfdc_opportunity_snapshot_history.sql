WITH sfdc_opportunity_snapshots AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_snapshots_base')}}

), final AS (

    SELECT
      -- keys
      date_actual,
      valid_from,
      valid_to,
      is_currently_valid,
      opportunity_snapshot_id,
      accountid                      AS account_id,
      id                             AS opportunity_id,
      name                           AS opportunity_name,
      ownerid                        AS owner_id,

      -- logistical information
      business_type__c               AS business_type,
      closedate                      AS close_date,
      createddate                    AS created_date,
      deployment_preference__c       AS deployment_preference,
      sql_source__c                  AS generated_source,
      leadsource                     AS lead_source,
      merged_opportunity__c          AS merged_opportunity_id,
      opportunity_owner__c           AS opportunity_owner,
      owner_team_o__c                AS opportunity_owner_team,
      account_owner__c               AS opportunity_owner_manager,
      sales_market__c                AS opportunity_owner_department,
      SDR_LU__c                      AS opportunity_sales_development_representative,
      BDR_LU__c                      AS opportunity_business_development_representative,
      account_owner_team_o__c        AS account_owner_team_stamped,
      COALESCE({{ sales_segment_cleaning('ultimate_parent_sales_segment_emp_o__c') }}, {{ sales_segment_cleaning('ultimate_parent_sales_segment_o__c') }} )
                                     AS parent_segment,
      sales_accepted_date__c         AS sales_accepted_date,
      engagement_type__c             AS sales_path,
      sales_qualified_date__c        AS sales_qualified_date,
      COALESCE({{ sales_segment_cleaning('sales_segmentation_employees_o__c') }}, {{ sales_segment_cleaning('sales_segmentation_o__c') }}, 'Unknown' )
                                     AS sales_segment,
      type                           AS sales_type,
      {{  sfdc_source_buckets('leadsource') }}
      stagename                      AS stage_name,

      -- opportunity information
      acv_2__c                       AS acv,
      IFF(acv_2__c >= 0, 1, 0)       AS closed_deals, -- so that you can exclude closed deals that had negative impact
      competitors__c                 AS competitors,
      critical_deal_flag__c          AS critical_deal_flag,
      {{sfdc_deal_size('incremental_acv_2__c', 'deal_size')}},
      forecastcategoryname           AS forecast_category_name,
      incremental_acv_2__c           AS forecasted_iacv,
      iacv_created_date__c           AS iacv_created_date,
      incremental_acv__c             AS incremental_acv,
      is_refund_opportunity__c       AS is_refund,
      is_downgrade_opportunity__c    AS is_downgrade,
      swing_deal__c                  AS is_swing_deal,
      net_iacv__c                    AS net_incremental_acv,
      nrv__c                         AS nrv,
      campaignid                     AS primary_campaign_source_id,
      probability                    AS probability,
      professional_services_value__c AS professional_services_value,
      push_counter__c                AS pushed_count,
      reason_for_lost__c             AS reason_for_loss,
      reason_for_lost_details__c     AS reason_for_loss_details,
      refund_iacv__c                 AS refund_iacv,
      downgrade_iacv__c              AS downgrade_iacv,
      renewal_acv__c                 AS renewal_acv,
      renewal_amount__c              AS renewal_amount,
      sql_source__c                  AS sales_qualified_source,
      sales_segmentation_o__c        AS segment,
      solutions_to_be_replaced__c    AS solutions_to_be_replaced,
      amount                         AS total_contract_value,
      upside_iacv__c                 AS upside_iacv,
      upside_swing_deal_iacv__c      AS upside_swing_deal_iacv,
      web_portal_purchase__c         AS is_web_portal_purchase,
      opportunity_term__c            AS opportunity_term,

      -- command plan fields
      fm_champion__c                 AS cp_champion,
      fm_close_plan__c               AS cp_close_plan,
      fm_competition__c              AS cp_competition,
      fm_decision_criteria__c        AS cp_decision_criteria,
      fm_decision_process__c         AS cp_decision_process,
      fm_economic_buyer__c           AS cp_economic_buyer,
      fm_identify_pain__c            AS cp_identify_pain,
      fm_metrics__c                  AS cp_metrics,
      fm_risks__c                    AS cp_risks,
      fm_use_cases__c                AS cp_use_cases,
      fm_value_driver__c             AS cp_value_driver,
      fm_why_do_anything_at_all__c   AS cp_why_do_anything_at_all,
      fm_why_gitlab__c               AS cp_why_gitlab,
      fm_why_now__c                  AS cp_why_now,
    
      -- metadata
      convert_timezone('America/Los_Angeles',convert_timezone('UTC',
               CURRENT_TIMESTAMP())) AS _last_dbt_run,
      isdeleted                      AS is_deleted,
      lastactivitydate               AS last_activity_date,
      recordtypeid                   AS record_type_id  
    FROM sfdc_opportunity_snapshots

)

SELECT *
FROM final