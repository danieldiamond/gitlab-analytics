{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

with ss_opportunity as (
  SELECT * FROM {{ ref('sfdc_snapshot_opportunity') }}

), account as (

    SELECT * FROM {{ ref('sfdc_account') }}

),oppstage as (

    SELECT * FROM {{ ref('sfdc_opportunity_stage') }}
),

transformed as (

        SELECT o.sfdc_opportunity_id                   AS opportunity_id,
               o.record_type_id,
               o.snapshot_date :: date                 AS snapshot_date,
               a.account_id                            AS account_id,
               s.stage_id                              AS opportunity_stage_id,
               COALESCE(o.sales_type, 'Unknown')       AS opportunity_type,
               o.sales_segment                         AS opportunity_sales_segmenation,
               o.sales_qualified_date :: date          AS sales_qualified_date,
               o.sales_accepted_date :: date           AS sales_accepted_date,
               o.generated_source                      AS sales_qualified_source,
               o.close_date :: date                    AS opportunity_closedate,
               COALESCE(o.opportunity_name, 'Unknown') AS opportunity_name,
               o.reason_for_loss,
               o.reason_for_loss_details,
               o.incremental_acv                       AS iacv,
               o.renewal_acv                           AS renewal_acv,
               o.acv,
               o.total_contract_value                  AS tcv,
               o.owner_id                              AS ownerid
        FROM ss_opportunity o
          INNER JOIN oppstage s ON o.stage_name = s.primary_label
          INNER JOIN account a ON o.account_id = a.account_id

 )

SELECT *
FROM transformed
