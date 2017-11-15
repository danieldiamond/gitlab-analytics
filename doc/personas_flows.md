# UX Personas and Workflows

We have identified two primary personas:
* An administrator who is responsible for initial setup and configuration of the data sources. This person could be a developer, or a bizops person, who has the access and credentials to add new data sources for integration.
* A user, who can be anyone in the organization. This user primarily consumes the metrics and data available within [the BI dashboard](../readme.md#tools), to help drive better decision making across the organization.

## Admin Flow

1. Start a new project with the BizOps template.
1. Add salesforce app credentials as environment variables.
1. Redeploy and look at a link that shows salesforce metadata. (can we make redeploy something that happens after setting environmental variables)
1. Use metadata to populate transform.yml and commit to master
1. Redeploy happens automatically and you see a insightsquared like graph.
  1. ELT runs and outputs into PG.
  1. Superset is then with PG as data source, with a set of dashboards loaded from a file in the repo.
  1. URL is set to be location of Superset

## User Flow

1. Measure the relationship between audience, content, and customers.
1. Generate variations on audience and content.
1. Find out if these lower CAC or produce a higher LTV.
1. Move your spend to the ones that perform better.
1. Repeat.

## Interaction Flow

Both marketing, sales, and customer success people are shown:
* Show the CAC and LTV of cohort/account/lead, what it is based on (how much / when) and the history of touchpoints.
* Actions (automatically scheduled ones, possible manual ones) with the calculated lift in LTV of each.

New actions can be programmed:
* Can be ads, an outbound message
* Can be triggered manually or automatic
* Can be dependent on doing something else first (need to watch video first, etc.)
