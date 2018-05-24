{{
  config({
    "materialized":"table",
    "post-hook": [
       "DROP INDEX IF EXISTS {{ this.schema }}.idx_f_opportunity_stageid",
       "DROP INDEX IF EXISTS {{ this.schema }}.idx_f_opportunity_closedate",
       "DROP INDEX IF EXISTS {{ this.schema }}.idx_f_opportunity_leadource",
       "DROP INDEX IF EXISTS {{ this.schema }}.idx_f_opportunity_account_id",
       "CREATE INDEX IF NOT EXISTS idx_f_opportunity_stageid ON {{ this }}(opportunity_stage_id)",
       "CREATE INDEX IF NOT EXISTS idx_f_opportunity_closedate ON {{ this}}(opportunity_closedate)",
       "CREATE INDEX IF NOT EXISTS idx_f_opportunity_leadource ON {{ this }}(lead_source_id)",
       "CREATE INDEX IF NOT EXISTS idx_f_opportunity_account_id ON {{ this }}(account_id)"
    ]
  })
}}


with lineitems as (
		select * from {{ ref('lineitems') }}

),
oppstage as (
		select * from {{ ref('dim_opportunitystage') }}

),

opportunity as (
	select * from {{ ref('opportunity') }}

),

leadsource as (
       select * from {{ ref('dim_leadsource') }}
),

account as (
       select * from {{ ref('dim_account') }}
)

SELECT o.sfdc_id AS opportunity_id
       , a.id AS account_id
       , s.id AS opportunity_stage_id
       , l.id AS lead_source_id
       , o.weighted_iacv__c AS weighted_iacv
       , COALESCE(o.type, 'Unknown') AS opportunity_type
       , COALESCE(o.sales_segmentation_o__c, 'Unknown') as opportunity_sales_segmentation
       , o.sales_qualified_date__c as sales_qualified_date
       , o.sales_accepted_date__c as sales_accepted_date
       , o.sql_source__c as sales_qualified_source
       , o.closedate AS opportunity_closedate
       , COALESCE(i.product, 'Unknown') as opportunity_product
       , COALESCE(i.period, 'Unknown') as billing_period
       , COALESCE(o.name, 'Unknown') as opportunity_name
       , o.ownerid
       , i.qty as quantity
       , i.iacv
       , i.renewal_acv
       , i.acv
       , i.tcv
FROM lineitems i
INNER JOIN opportunity o ON i.opportunity_id=o.sfdc_id
INNER JOIN oppstage s ON o.stagename=s.masterlabel
INNER JOIN leadsource l on o.leadsource=l.Initial_Source
INNER JOIN account a on o.accountId=a.sfdc_account_id