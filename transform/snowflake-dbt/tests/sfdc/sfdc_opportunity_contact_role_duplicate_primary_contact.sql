-- Fails if there are opportunities with multiple primary contacts

SELECT
    opportunity_id,
    COUNT(1) counts
FROM {{ref('sfdc_opportunity_contact_role')}}
WHERE is_primary = 1
GROUP BY 1
HAVING COUNT(1) > 1
