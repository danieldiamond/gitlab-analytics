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
      WHEN {{ lead_source }} in ('Web Chat', 'Request - Public Sector', 'Request - Professional Services', 'Request - Contact', 'Email Request')
        THEN 'Inbound Request'
      WHEN {{ lead_source }} in ('Gated Content - General', 'Gated Content - Report', 'Gated Content - Video', 'Gated Content - Whitepaper')
        THEN 'Gated Content'
      WHEN {{ lead_source }} in ('Conference', 'Field Event', 'Live Event', 'Owned Event', 'Virtual Sponsorship')
        THEN 'Field Marketing'
      WHEN {{ lead_source }} in ('Prospecting - LeadIQ', 'Prospecting - General', 'Prospecting', 'LinkedIn', 'Leadware', 'Datanyze', 'DiscoverOrg', 'Clearbit')
        THEN 'Prospecting'
      WHEN {{ lead_source }} in ('Demo')
        THEN 'Demo'
      WHEN {{ lead_source }} in ('Trial - Enterprise')
        THEN 'Trial - Self-managed'
      WHEN {{ lead_source }} in ('Trial - GitLab.com')
        THEN 'Trial - SaaS (GitLab.com)'
      WHEN {{ lead_source }} in ('SDR Generated')
        THEN 'SDR Generated'
      WHEN {{ lead_source }} in ('AE Generated')
        THEN 'AE Generated'
      WHEN {{ lead_source }} in ('Webcast')
        THEN 'Webcast'
      WHEN {{ lead_source }} in ('Web Direct', 'Web')
        THEN 'Web Direct'
      WHEN {{ lead_source }} in ('Newsletter', 'Security Newsletter', 'Email Subscription')
        THEN 'Newsletter'
      WHEN {{ lead_source }} in ('Employee Referral', 'External Referral')
        THEN 'Referral'
      WHEN {{ lead_source }} in ('GitLab.com', 'Gitlab.com')
        THEN 'GitLab.com'
      WHEN {{ lead_source }} in ('OSS', 'Education')
        THEN 'EDU/OSS'
      ELSE 'Other'
    END                               AS source_buckets,

{%- endmacro -%}
