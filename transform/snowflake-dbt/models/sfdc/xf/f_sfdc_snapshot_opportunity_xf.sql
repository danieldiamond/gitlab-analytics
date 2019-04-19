{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH ss_opportunity AS (

    SELECT * FROM {{ ref('sfdc_snapshot_opportunity') }}

), account AS (

    SELECT * FROM {{ ref('sfdc_account') }}

),oppstage AS (

    SELECT * FROM {{ ref('sfdc_opportunity_stage') }}

), leadsource AS (

    SELECT * FROM {{ ref('sfdc_lead_source') }} 
)

SELECT ss_opportunity.sfdc_opportunity_id                       AS opportunity_id,
       ss_opportunity.record_type_id,
	   ss_opportunity.snapshot_date::date                       AS snapshot_date,
       COALESCE(ss_opportunity.sales_type, 'Unknown')           AS opportunity_type,
       ss_opportunity.sales_segment                             AS opportunity_sales_segmenation,
       ss_opportunity.sales_qualified_date::date                AS sales_qualified_date,
       ss_opportunity.sales_accepted_date::date                 AS sales_accepted_date,
       ss_opportunity.generated_source                          AS sales_qualified_source,
       ss_opportunity.close_date::date                          AS opportunity_closedate,
       COALESCE(ss_opportunity.opportunity_name, 'Unknown')     AS opportunity_name,
       ss_opportunity.reason_for_loss,
       ss_opportunity.reason_for_loss_details,
       ss_opportunity.incremental_acv                           AS iacv,
       ss_opportunity.renewal_acv                               AS Renewal_ACV,
       ss_opportunity.acv,
       ss_opportunity.total_contract_value                      AS tcv,
       ss_opportunity.owner_id                                  AS ownerid,
       account.id                                               AS account_id,
       oppstage.stage_id                                        AS opportunity_stage_id,
       leadsource.id                                            AS lead_source_id
FROM ss_opportunity
INNER JOIN oppstage 
    ON ss_opportunity.stage_name = oppstage.primary_label
INNER JOIN leadsource 
    ON ss_opportunity.lead_source = leadsource.Initial_Source
INNER JOIN account 
    ON ss_opportunity.account_id = account.sfdc_account_id
