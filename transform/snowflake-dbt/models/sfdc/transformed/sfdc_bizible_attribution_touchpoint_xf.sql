WITH opps AS (
    SELECT *
    FROM {{ref('sfdc_opportunity_xf')}}
),

    touches AS (
      SELECT *
      FROM {{ref('sfdc_bizible_attribution_touchpoint')}}
  )


    SELECT
      touches.*,
      CASE
        WHEN marketing_channel_path = 'CPC.AdWords'
          THEN 'Google AdWords'
        WHEN marketing_channel_path IN ('Email.Other', 'Email.Newsletter','Email.Outreach')
          THEN 'Email'
        WHEN marketing_channel_path IN ('Field Event','Partners.Google','Brand.Corporate Event','Conference')
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
          THEN 'Organic'
        WHEN marketing_channel_path IN ('Sponsorship','Speaking Session')
          THEN 'Paid Sponsorship'
        ELSE 'Unknown'
              END                                                   AS pipe_name,
      opps.incremental_acv * touches.attribution_percent_full_path  AS iacv_full_path
    FROM opps
      JOIN touches ON touches.opportunity_id = opps.opportunity_id
