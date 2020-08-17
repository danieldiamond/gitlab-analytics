{%- macro sfdc_source_buckets(lead_source) -%}

    CASE
      WHEN {{ lead_source }} in ('CORE Check-Up')
        THEN 'Core'
      WHEN {{ lead_source }} in ('GitLab Subscription Portal', 'Gitlab.com', 'GitLab.com', 'Trial - Gitlab.com', 'Trial - GitLab.com')
        THEN 'GitLab.com'
      WHEN {{ lead_source }} in ('Education', 'OSS')
        THEN 'Marketing/Community'
      WHEN {{ lead_source }} in ('CE Download', 'Demo', 'Drift', 'Email Request', 'Email Subscription', 'Gated Content - General', 'Gated Content - Report', 'Gated Content - Video'
                           , 'Gated Content - Whitepaper', 'Live Event', 'Newsletter', 'Request - Contact', 'Request - Professional Services', 'Request - Public Sector'
                           , 'Security Newsletter', 'Trial - Enterprise', 'Virtual Sponsorship', 'Web Chat', 'Web Direct', 'Web', 'Webcast')
        THEN 'Marketing/Inbound'
      WHEN {{ lead_source }} in ('Advertisement', 'Conference', 'Field Event', 'Owned Event')
        THEN 'Marketing/Outbound'
      WHEN {{ lead_source }} in ('Clearbit', 'Datanyze', 'Leadware', 'LinkedIn', 'Prospecting - LeadIQ', 'Prospecting - General', 'Prospecting', 'SDR Generated')
        THEN 'Prospecting'
      WHEN {{ lead_source }} in ('Employee Referral', 'External Referral', 'Partner', 'Word of mouth')
        THEN 'Referral'
      WHEN {{ lead_source }} in ('AE Generated')
        THEN 'Sales'
      WHEN {{ lead_source }} in ('DiscoverOrg')
        THEN 'DiscoverOrg'
      ELSE 'Other'
    END                               AS net_new_source_categories,
   CASE
      WHEN {{ lead_source }} in ('CORE Check-Up', 'CE Download', 'CE Usage Ping','CE Version Check')
        THEN 'core'
      WHEN {{ lead_source }} in ('Consultancy Request','Contact Request','Content','Demo','Drift','Education','EE Version Check','Email Request','Email Subscription','Enterprise Trial','Gated Content - eBook','Gated Content - General','Gated Content - Report','Gated Content - Video','Gated Content - Whitepaper','GitLab.com','MovingtoGitLab','Newsletter','OSS','Request - Community','Request - Contact','Request - Professional Services','Request - Public Sector','Security Newsletter','Startup Application','Trial - Enterprise','Trial - GitLab.com','Web','Web Chat','White Paper')
        THEN 'inbound'
      WHEN {{ lead_source }} in ('AE Generated', 'Clearbit','Datanyze','DiscoverOrg','Gemnasium','GitLab Hosted','Gitorious','gmail','Leadware','LinkedIn','Live Event','Prospecting','Prospecting - General','Prospecting - LeadIQ','SDR Generated','seamless.ai','Zoominfo')
        THEN 'outbound'
      WHEN {{ lead_source }} in ('Advertisement', 'Conference', 'Executive Roundtable', 'Field Event', 'Owned Event','Promotion','Virtual Sponsorship')
        THEN 'paid demand gen'
      WHEN {{ lead_source }} in ('Purchased List')
        THEN 'purchased list'
      WHEN {{ lead_source }} in ('Employee Referral', 'Event Partner', 'Existing Client', 'External Referral','Partner','Seminar - Partner','Word of mouth')
        THEN 'referral'
      WHEN {{ lead_source }} in ('Webcast','Webinar')
        THEN 'virtual event'
      WHEN {{ lead_source }} in ('GitLab Subscription Portal','Web Direct')
        THEN 'web direct'
      ELSE 'Other'
    END                               AS source_buckets,

{%- endmacro -%}
