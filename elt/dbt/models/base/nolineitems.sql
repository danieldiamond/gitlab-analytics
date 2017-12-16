
with quotelineitems as (
    SELECT opportunity_id
           FROM {{ ref('quotelineitems') }} 
),

opplineitems as (
  SELECT opportunity_id
              FROM  {{ ref('opplineitems') }} 
)

SELECT o.id,
       o.id AS opportunity_id,
       COALESCE(o.products_purchased__c, 'Unknown'::text) AS product,
       'Unknown'::text AS period,
       1 AS qty,
       o.incremental_acv_2__c AS iacv,
       COALESCE(o.mrr1__c, o.mrr__c) AS mrr
FROM sfdc.opportunity o
WHERE o.isdeleted = FALSE
  AND NOT (o.id::text IN
             (SELECT opportunity_id
              FROM quotelineitems))
  AND NOT (o.id::text IN
             (SELECT opportunity_id
              FROM opplineitems))