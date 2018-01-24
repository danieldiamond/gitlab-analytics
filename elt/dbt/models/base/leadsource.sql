SELECT distinct LeadSource from sfdc.opportunity where isdeleted='false'
UNION  
SELECT distinct LeadSource from sfdc.lead where isdeleted='false'
UNION
SELECT distinct LeadSource from sfdc.contact where isdeleted='false'