SELECT distinct LeadSource 
FROM raw.salesforce_stitch.opportunity 
WHERE isdeleted='false' 
AND LeadSource IS NOT NULL

UNION  

SELECT distinct LeadSource 
FROM raw.salesforce_stitch.lead 
WHERE isdeleted='false' 
AND LeadSource IS NOT NULL

UNION

SELECT distinct LeadSource 
FROM raw.salesforce_stitch.contact 
WHERE isdeleted='false' 
AND LeadSource IS NOT NULL