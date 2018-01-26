SELECT id AS sfdc_id,
	   accountid,
       stagename,
       leadsource,
       TYPE,
       closedate,
       sql_source__c,
       sales_segmentation_o__c,
       sales_qualified_date__c,
       name

FROM sfdc.opportunity
WHERE isdeleted=FALSE