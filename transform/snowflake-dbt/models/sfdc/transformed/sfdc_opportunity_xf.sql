{{ config(schema='analytics') }}

WITH sfdc_opportunity AS ( 

    SELECT * FROM {{ref('sfdc_opportunity')}}

), sfdc_opportunitystage AS (

    SELECT * FROM {{ref('sfdc_opportunitystage')}}

), lead_source AS (

    SELECT * FROM {{ref('dim_leadsource')}}

), sfdc_users_xf AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

), sfdc_record_type as (

     SELECT *
     FROM {{ ref('sfdc_record_type') }}    

), layered AS (

    SELECT
        sfdc_opportunity.*,
        sfdc_opportunity.incremental_acv * (sfdc_opportunitystage.default_probability /100)     as weighted_iacv,
        md5((date_trunc('month', sfdc_opportunity.close_date)::date)||UPPER(sfdc_users_xf.name))   as sales_quota_id,
        md5((date_trunc('month', sfdc_opportunity.close_date)::date)||UPPER(sfdc_users_xf.team))   as region_quota_id,
        lead_source.id                                                                          as lead_source_id,
        lead_source.initial_source                                                              as lead_source_name,
        lead_source.initial_source_type                                                         as lead_source_type,
        sfdc_opportunitystage.is_won                                                            as is_won,
        sfdc_opportunitystage.default_probability                                               as stage_default_probability,
        sfdc_opportunitystage.is_active                                                         as stage_is_active,
        sfdc_opportunitystage.is_closed                                                         as stage_is_closed,
        sfdc_opportunitystage.stage_state                                                       as stage_state,
        sfdc_opportunitystage.mapped_stage                                                      as mapped_stage,
        sfdc_users_xf.name                                                                         as opportunity_owner,
        sfdc_users_xf.team                                                                         as opportunity_owner_team,
        sfdc_users_xf.manager_name                                                                 as opportunity_owner_manager,
        sfdc_users_xf.department                                                                   as opportunity_owner_department,
        sfdc_users_xf.title                                                                        as opportunity_owner_title,
        sfdc_users_xf.role_name                                                                    as opportunity_owner_role,
        sfdc_users_xf.employee_tags                                                                as opportunity_owner_tags,
        sfdc_record_type.record_type_name,
        sfdc_record_type.business_process_id,
        sfdc_record_type.record_type_label,
        sfdc_record_type.record_type_description,
        sfdc_record_type.record_type_modifying_object_type,
        CASE WHEN (sfdc_opportunity.days_in_stage > 30
          	OR sfdc_opportunity.incremental_acv > 100000 
          	OR sfdc_opportunity.pushed_count > 0)
            THEN TRUE
          ELSE FALSE
         END AS is_risky
    FROM sfdc_opportunity
    INNER JOIN sfdc_opportunitystage on sfdc_opportunity.stage_name = sfdc_opportunitystage.primary_label
    INNER JOIN lead_source on sfdc_opportunity.lead_source = lead_source.Initial_Source
    LEFT JOIN sfdc_users_xf ON sfdc_opportunity.owner_id = sfdc_users_xf.id
    LEFT JOIN sfdc_record_type ON sfdc_opportunity.record_type_id = sfdc_record_type.record_type_id

)

SELECT *
FROM layered
