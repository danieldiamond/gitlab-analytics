WITH sfdc_opportunity AS (

    SELECT * FROM {{ref('sfdc_opportunity')}}

), sfdc_opportunitystage AS (

    SELECT * FROM {{ref('sfdc_opportunity_stage')}}

), sfdc_lead_source AS (

    SELECT * FROM {{ref('sfdc_lead_source')}}

), sfdc_users_xf AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}

), layered AS (

    SELECT
      -- keys
      sfdc_opportunity.account_id,
      sfdc_opportunity.opportunity_id,
      sfdc_opportunity.opportunity_name,
      sfdc_opportunity.owner_id,

      -- logistical information
      sfdc_opportunity.business_type,
      sfdc_opportunity.close_date,
      sfdc_opportunity.created_date,
      sfdc_opportunity.days_in_stage,
      sfdc_opportunity.deployment_preference,
      sfdc_opportunity.generated_source,
      sfdc_opportunity.lead_source,
      sfdc_lead_source.lead_source_id                                                             AS lead_source_id,
      COALESCE(sfdc_lead_source.initial_source, 'Unknown')                                        AS lead_source_name,
      COALESCE(sfdc_lead_source.initial_source_type, 'Unknown')                                   AS lead_source_type,
      sfdc_opportunitystage.mapped_stage                                                          AS mapped_stage,
      sfdc_opportunity.merged_opportunity_id,
      sfdc_opportunity.net_new_source_categories,
      sfdc_opportunity.opportunity_business_development_representative,
      sfdc_opportunity.opportunity_owner                                                          AS opportunity_owner,
      sfdc_opportunity.opportunity_owner_department                                               AS opportunity_owner_department,
      sfdc_opportunity.opportunity_owner_manager                                                  AS opportunity_owner_manager,
      sfdc_users_xf.role_name                                                                     AS opportunity_owner_role,
      sfdc_opportunity.opportunity_owner_team                                                     AS opportunity_owner_team,
      sfdc_users_xf.title                                                                         AS opportunity_owner_title,
      sfdc_opportunity.opportunity_sales_development_representative,
      sfdc_opportunity.opportunity_term,
      sfdc_opportunity.parent_segment,
      sfdc_opportunity.primary_campaign_source_id                                                 AS primary_campaign_source_id,
      sfdc_opportunity.sales_accepted_date,
      sfdc_opportunity.sales_path,
      sfdc_opportunity.sales_qualified_date,
      sfdc_opportunity.sales_segment,
      sfdc_opportunity.sales_type,
      sfdc_opportunity.source_buckets,
      sfdc_opportunitystage.default_probability                                                   AS stage_default_probability,
      sfdc_opportunitystage.is_active                                                             AS stage_is_active,
      sfdc_opportunitystage.is_closed                                                             AS stage_is_closed,
      sfdc_opportunity.stage_name,
      sfdc_opportunitystage.stage_state                                                           AS stage_state,

      -- opportunity information
      sfdc_opportunity.acv,
      sfdc_opportunity.closed_deals,
      sfdc_opportunity.competitors,
      sfdc_opportunity.deal_size,
      sfdc_opportunity.forecast_category_name,
      sfdc_opportunity.forecasted_iacv,
      sfdc_opportunity.iacv_created_date,
      sfdc_opportunity.incremental_acv,
      sfdc_opportunity.is_refund,
      sfdc_opportunity.is_downgrade,
      CASE WHEN (sfdc_opportunity.days_in_stage > 30
        OR sfdc_opportunity.incremental_acv > 100000
        OR sfdc_opportunity.pushed_count > 0)
      THEN TRUE
      ELSE FALSE
      END                                                                                         AS is_risky,
      sfdc_opportunity.is_swing_deal,
      sfdc_opportunitystage.is_won                                                                AS is_won,
      sfdc_opportunity.net_incremental_acv,
      sfdc_opportunity.nrv,
      sfdc_opportunity.professional_services_value,
      sfdc_opportunity.pushed_count,
      sfdc_opportunity.reason_for_loss,
      sfdc_opportunity.reason_for_loss_details,
      sfdc_opportunity.refund_iacv,
      sfdc_opportunity.downgrade_iacv,
      sfdc_opportunity.renewal_acv,
      sfdc_opportunity.renewal_amount,
      sfdc_opportunity.sales_qualified_source,
      sfdc_opportunity.solutions_to_be_replaced,
      sfdc_opportunity.total_contract_value,
      sfdc_opportunity.upside_iacv,
      sfdc_opportunity.upside_swing_deal_iacv,
      sfdc_opportunity.incremental_acv * (sfdc_opportunitystage.default_probability /100)         AS weighted_iacv,
      sfdc_opportunity.is_web_portal_purchase,

      -- metadata
      sfdc_opportunity._last_dbt_run,
      sfdc_record_type.business_process_id,
      sfdc_opportunity.days_since_last_activity,
      sfdc_opportunity.is_deleted,
      sfdc_opportunity.last_activity_date,
      sfdc_record_type.record_type_description,
      sfdc_opportunity.record_type_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_modifying_object_type,
      sfdc_record_type.record_type_name,
      md5((date_trunc('month', sfdc_opportunity.close_date)::date)||UPPER(sfdc_users_xf.team))    AS region_quota_id,
      md5((date_trunc('month', sfdc_opportunity.close_date)::date)||UPPER(sfdc_users_xf.name))    AS sales_quota_id

  FROM sfdc_opportunity
  INNER JOIN sfdc_opportunitystage
    ON sfdc_opportunity.stage_name = sfdc_opportunitystage.primary_label
  LEFT JOIN sfdc_lead_source
    ON sfdc_opportunity.lead_source = sfdc_lead_source.initial_source
  LEFT JOIN sfdc_users_xf
    ON sfdc_opportunity.owner_id = sfdc_users_xf.id
  LEFT JOIN sfdc_record_type
    ON sfdc_opportunity.record_type_id = sfdc_record_type.record_type_id

)

SELECT *
FROM layered
