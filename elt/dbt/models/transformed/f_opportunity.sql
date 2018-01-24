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
       , o.type AS opportunity_type
       , o.sales_segmentation_o__c as opportunity_sales_segmentation
       , o.sales_qualified_date__c as sales_qualified_date
       , o.sql_source__c as sales_qualified_source
       , o.closedate AS opportunity_closedate
       , i.product as opportunity_product
       , i.period as billing_period
       , i.qty as quantity
       , i.iacv
FROM lineitems i
INNER JOIN opportunity o ON i.opportunity_id=o.sfdc_id
INNER JOIN oppstage s ON o.stagename=s.masterlabel
INNER JOIN leadsource l on o.leadsource=l.Initial_Source
INNER JOIN account a on o.accountId=a.sfdc_account_id