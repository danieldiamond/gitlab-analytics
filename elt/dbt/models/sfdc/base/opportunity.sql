SELECT
  id             AS sfdc_id,
  accountid,
  stagename,
  leadsource,
  TYPE,
  closedate,
  sql_source__c,
  sales_segmentation_o__c,
  sales_qualified_date__c,
  sales_accepted_date__c,
  name,
  ownerid,
  weighted_iacv__c,
  current_date - greatest(
      x0_pending_acceptance_date__c,
      x1_discovery_date__c,
      x2_scoping_date__c,
      x3_technical_evaluation_date__c,
      x4_proposal_date__c,
      x5_negotiating_date__c,
      x6_closed_won_date__c,
      x7_closed_lost_date__c,
      x8_unqualified_date__c
  ) :: DATE + 1  AS days_in_stage,
  CASE
  WHEN incremental_acv__c > 100000
    THEN TRUE
  ELSE FALSE END AS over_100k,
  push_counter__c
FROM sfdc.opportunity
WHERE isdeleted = FALSE