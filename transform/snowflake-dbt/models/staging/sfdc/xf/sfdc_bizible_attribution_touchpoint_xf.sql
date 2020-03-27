WITH opps AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_xf')}}

), touches AS (

      SELECT *
      FROM {{ref('sfdc_bizible_attribution_touchpoint')}}
      WHERE isdeleted = FALSE

), final AS (

    SELECT
      date_trunc('month', opps.sales_accepted_date) AS sales_accepted_month,
      date_trunc('month', opps.sales_qualified_date)                 AS sales_qualified_month,
      date_trunc('month', opps.close_date) as close_month,
      touches.*,
      CASE
        WHEN touchpoint_id ILIKE 'a6061000000CeS0%' -- Specific touchpoint overrides
          THEN 'Field Event'
        WHEN marketing_channel_path = 'CPC.AdWords'
          THEN 'Google AdWords'
        WHEN marketing_channel_path IN ('Email.Other', 'Email.Newsletter','Email.Outreach')
          THEN 'Email'
        WHEN marketing_channel_path IN ('Field Event','Partners.Google','Brand.Corporate Event','Conference','Speaking Session')
                  OR (bizible_medium = 'Field Event (old)' AND marketing_channel_path = 'Other')
          THEN 'Field Event'
        WHEN marketing_channel_path IN ('Paid Social.Facebook','Paid Social.LinkedIn','Paid Social.Twitter','Paid Social.YouTube')
          THEN 'Paid Social'
        WHEN marketing_channel_path IN ('Social.Facebook','Social.LinkedIn','Social.Twitter','Social.YouTube')
          THEN 'Social'
        WHEN marketing_channel_path IN ('Marketing Site.Web Referral','Web Referral')
          THEN 'Web Referral'
        WHEN marketing_channel_path in ('Marketing Site.Web Direct', 'Web Direct')
              -- Added to Web Direct
              OR campaign_id in (
                                '701610000008ciRAAQ', -- Trial - GitLab.com
                                '70161000000VwZbAAK', -- Trial - Self-Managed
                                '70161000000VwZgAAK', -- Trial - SaaS
                                '70161000000CnSLAA0', -- 20181218_DevOpsVirtual
                                '701610000008cDYAAY'  -- 2018_MovingToGitLab
                                )
          THEN 'Web Direct'
        WHEN marketing_channel_path LIKE 'Organic Search.%'
              OR marketing_channel_path = 'Marketing Site.Organic'
          THEN 'Organic Search'
        WHEN marketing_channel_path IN ('Sponsorship')
          THEN 'Paid Sponsorship'
        ELSE 'Unknown'
              END                                                           AS pipe_name,
      opps.incremental_acv * touches.bizible_attribution_percent_full_path  AS iacv_full_path,
      opps.sales_type,
      opps.lead_source,
      opps.record_type_label
    FROM opps
    INNER JOIN touches 
    ON touches.opportunity_id = opps.opportunity_id

)

SELECT * 
FROM final