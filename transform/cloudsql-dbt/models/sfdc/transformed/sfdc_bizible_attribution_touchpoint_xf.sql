WITH opps AS (
    SELECT *
    FROM {{ref('sfdc_opportunity_xf')}}
),

    touches AS (
      SELECT *
      FROM {{ref('sfdc_bizible_attribution_touchpoint')}}
  )


    SELECT
      date_trunc('month',opps.sales_qualified_date)                 AS sales_qualified_month,
      touches.*,
      CASE
        WHEN touchpoint_id ILIKE 'a6061000000CeS0%' -- Specific touchpoing overrides
              THEN 'Field Event'
        WHEN marketing_channel_path = 'CPC.AdWords'
              THEN 'Google Adwords'
        WHEN marketing_channel_path IN ('Email.Other', 'Email.Newsletter','Email.Outreach')
          THEN 'Email'
        WHEN marketing_channel_path IN ('Field Event','Partners.Google','Brand.Corporate Event','Conference','Speaking Session')
                  OR (medium = 'Field Event (old)' AND marketing_channel_path = 'Other')
          THEN 'Field Event'
        WHEN marketing_channel_path IN ('Paid Social.Facebook','Paid Social.LinkedIn','Paid Social.Twitter','Paid Social.YouTube')
          THEN 'Paid Social'
        WHEN marketing_channel_path IN ('Social.Facebook','Social.LinkedIn','Social.Twitter','Social.YouTube')
          THEN 'Social'
        WHEN marketing_channel_path = 'Marketing Site.Web Referral'
          THEN 'Web Referral'
        WHEN marketing_channel_path = 'Marketing Site.Web Direct'
          THEN 'Web Direct'
        WHEN marketing_channel_path = 'Marketing Site.Organic'
                  OR (medium = 'Marketing Site' AND marketing_channel_path = 'NULL')
          THEN 'Organic Search'
        WHEN marketing_channel_path IN ('Sponsorship')
          THEN 'Paid Sponsorship'
        ELSE 'Unknown'
              END                                                   AS pipe_name,
      opps.incremental_acv * touches.attribution_percent_full_path  AS iacv_full_path,
      opps.sales_type,
      opps.lead_source
    FROM opps
      JOIN touches ON touches.opportunity_id = opps.opportunity_id
