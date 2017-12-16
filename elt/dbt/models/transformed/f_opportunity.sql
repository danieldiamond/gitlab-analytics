with lineitems as (
		select * from {{ ref('lineitems') }}

),
oppstage as (
		select * from {{ ref('dim_opportunitystage') }}

),

opportunity as (
	select * from {{ ref('opportunity') }}

)

SELECT o.sfdc_id,
       s.id AS opportunity_stage_id,
       o.type AS opportunity_type,
       o.closedate AS opportunity_closedate,
       i.product as opportunity_product,
       i.period as billing_period,
       i.qty as quantity,
       i.iacv
FROM lineitems i
INNER JOIN opportunity o ON i.opportunity_id=o.sfdc_id
INNER JOIN oppstage s ON o.stagename=s.masterlabel