{{
  config(
    materialized ='table',
    schema='analytics'
  )
}}

with ss_opportunity as (
	  select * from {{ ref('snapshot_opportunity') }}
),

leadsource as (
    select * from {{ ref('dim_leadsource') }}
),

account as (
    select * from {{ ref('dim_account') }}
),

oppstage as (
		select * from {{ ref('dim_opportunitystage') }}

)

SELECT o.sfdc_opportunity_id AS opportunity_id
       , o.record_type_id
	     , o.snapshot_date::date AS snapshot_date
       , a.id AS account_id
       , s.stage_id AS opportunity_stage_id
       , l.id AS lead_source_id
       , COALESCE(o.sales_type, 'Unknown') AS opportunity_type
       , o.sales_segment AS opportunity_sales_segmenation
       , o.sales_qualified_date::date AS sales_qualified_date
       , o.sales_accepted_date::date AS sales_accepted_date
       , o.generated_source AS sales_qualified_source
       , o.close_date::date AS opportunity_closedate
       , COALESCE(o.opportunity_name, 'Unknown') as opportunity_name
       , o.reason_for_loss
       , o.reason_for_loss_details
       , o.incremental_acv AS iacv
       , o.renewal_acv AS Renewal_ACV
       , o.acv
       , o.total_contract_value AS tcv
       , o.owner_id AS ownerid
FROM  ss_opportunity o
INNER JOIN oppstage s ON o.stage_name=s.primary_label
INNER JOIN leadsource l on o.lead_source=l.Initial_Source
INNER JOIN account a on o.account_id=a.sfdc_account_id

