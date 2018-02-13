# Data Sources

We plan to support the following platforms:

1. Salesforce (in progress)
1. Zuora (in progress)
1. Marketo (in progress)
1. Zendesk
1. NetSuite
1. Mailchimp
1. Google Analytics
1. Discover.org
1. Clearbit
1. Lever
1. GitLab version check (in progress)
1. GitLab usage ping (in progress)
1. [GitLab.com](https://about.gitlab.com/handbook/engineering/workflow/#getting-data-about-gitlabcom) (in progress)

Data from these platforms is pulled into a single data warehouse with a [common data model](data_model.md). We bring all relevant data to a single data model so it can be used easily and consistently across tools and teams. For example something as simple as unique customer ID, product or feature names/codes.

We are however open to pragmatic solutions linking for example Salesforce and Zendesk, if there are boring solutions available we'll adopt them instead of creating our own.
