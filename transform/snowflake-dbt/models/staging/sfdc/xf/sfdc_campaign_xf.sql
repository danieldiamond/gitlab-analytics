WITH sfdc_campaign AS (

    SELECT *
    FROM {{ref('sfdc_campaign')}}

), xf AS (

    SELECT  sfdc_campaign.campaign_id,
             sfdc_campaign.campaign_name,
             sfdc_campaign.description,
             sfdc_campaign.type             AS campaign_type,
             sfdc_campaign.start_date       AS campaign_start_date,
             sfdc_campaign.end_date         AS campaign_end_date,
             parent_campaign.campaign_name  AS parent_campaign_name,
             parent_campaign.type           AS parent_campaign_type,
             sfdc_campaign.is_active,
             sfdc_campaign.amount_all_opportunities,
             sfdc_campaign.amount_won_opportunities,
             sfdc_campaign.count_contacts,
             sfdc_campaign.count_converted_leads,
             sfdc_campaign.count_leads,
             sfdc_campaign.count_opportunities,
             sfdc_campaign.count_responses,
             sfdc_campaign.count_sent,
             sfdc_campaign.count_won_opportunities
    FROM sfdc_campaign
    LEFT JOIN sfdc_campaign AS parent_campaign
    ON sfdc_campaign.campaign_parent_id = parent_campaign.campaign_id

)

SELECT *
FROM xf