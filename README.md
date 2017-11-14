# BizOps

BizOps is a convention-over-configuration framework for analytics, business intelligence, and data science. It leverages open source software and software development best practices including version control, CI, CD, and review apps.

## Objectives

1. Provide an open source BI tool to analyze sales and marketing performance ([in progress](#development-plan))
1. Expand into customer success insight and automation
1. Close the feedback loop between product feature launches and sales/marketing performance
1. Widen to include other areas of the company, like people ops and finance

## Development Status

BizOps is pre-alpha and under active development, focused on delivering [objective #1](#objectives). More information and current status is available in our [development plan](doc/development_plan.md).

## Analyzing Sales & Marketing Performance

As part of [objective #1](#objectives), we are targeting analytics for sales and marketing performance. We plan to track the following metrics per campaign, lead, and organization. These results will be able to reviewed over various time periods.

* Cost per lead = dollar spend / number of attributed leads
* Median conversion time (from touch to IACV)
* IACV at median conversion time (at for example 90 days after touch)
* Estimated IACV = 2 *  IACV at median conversion time
* CAC = cost per lead * conversion from lead to IACV
* LTV = IACV * margin * average retention time
* ROI = LTV / CAC
* Collateral usage

To achieve this, we bring data from all [data sources](data_sources.md) to a [common data model](doc/data_model.md) so it can be used easily and consistently across tools and teams. For example something as simple as unique customer ID, product or feature names/codes.

## Tools

We want the tools to be open source so we can ship this as a product.

1. Extract and Load (EL): [Talend](https://www.talend.com) for the ELT engine, although are considering [Singer](https://www.singer.io/) once it supports Salesforce and PostgreSQL.
1. Transformation: [dbt](https://docs.getdbt.com/) to handle transforming the raw data into a normalized data model within PG.
1. Warehouse: [PostgeSQL](https://www.postgresql.org/), maybe later with [a column extension](https://github.com/citusdata/cstore_fdw). If people need SaaS BigQuery is nice option and [Druid](http://druid.io/) seems like a good pure column oriented database.
1. Display/analytics: [Superset](https://github.com/airbnb/superset) to visualize the [metrics](#metrics).
1. Orchestration/Monitoring: [GitLab CI](https://about.gitlab.com/features/gitlab-ci-cd/) for scheduling, running, and monitoring the ELT jobs. Non-GitLab alternatives are [Airflow](https://airflow.incubator.apache.org) or [Luigi](https://github.com/spotify/luigi).

## How to use

We have identified [two personas](doc/personas_flows.md):
* An "administrator" who is reponsible for setting up the project and integrating data.
* And "users", employees at the company who can use the dashboard to improve decision making across the organization.

### Dockerfile
The image combines [Apache Superset](https://superset.incubator.apache.org/index.html) with a [PostgreSQL](https://www.postgresql.org/) database. It creates an image pre-configured to use a local PostgreSQL database and loads the sample data and reports. The setup.py file launches the database as well as starts the Superset service on port 8080 using [Gunicorn](http://gunicorn.org/).

### To launch the container:

1. Clone the repo and cd into it.
2. Edit the config/bizops.conf file to enter in a username, first name, last name, email and password for the Superset administrator.
3. Optionally, you can edit the user and password in the Dockerfile on line 35 to change the defaule postgres user/password. If you do this, you'll also need to update the SQLAlchemy url in the /config/superset_config.py file on line 21
4. Build the image with `docker build --rm=true -t bizops .`.
5. Run the image with `docker run -p 80:8088 bizops`.
6. Go to [http://localhost](http://localhost) and log in using the credentials you entered in step 2.

# Why open source BizOps within GitLab?

## Premise
* Conway's law, every functional group
* Based on open source tools
* Allow many companies to add their best practices
* Only makes sense to make as a product for everyone
* What GitLab does for DevOps this product should do for Revenue
* We want to sophistication of a recent public company without hiring a department of people.
* Will disinfect and give freedom for experimentation
* Have examples of detection
* Marketeer becomes trader
* Replace analytic tools
* Investors will insist on this
* First opinionated business framework
* Complete BizOps
* 100k organizations should create it together
* Can trivially replace other solutions when implemented
* We're starting with the integration.

### Why integrated

* Because the CAC depends on many touch points across many systems.
* Because the LTV depends on many variables.
* Because the CAC and LTV are specific for a customer and depend on many systems.
* Because product should inform sales efforts.
* Because nobody can currently tell me the CAC of specific account.
* Because nobody can currently can predict the lift in LTV of a specific action.

### Examples

* Show financials an ad about compliance.
* If product shows someone just started to use CI show ads with CI information.
* Add logo's from the same industry to an auto generated presentation

### Competition & Value

This should be a replacement for:
* Marketing automation (Marketo)
* Sales analytics (Insightsquared)
* Customer Success automation (Gainsight)

You should also be able to retire:
* Analytics (Tableau)
* Sales content management (Highspot/Docsend)

In the beginning the software should build on some existing systems, for example the drip campaigns in Marketo.

### Concepts

* Account based marketing
* Auto generate a presentation
* Correlation of events with revenue
* Lift generated by ads (a/b testing)
* Uploading emails for a custom audience
* Cross device tracking
* Need thousands of people to create a lookalike audience.
* Best ad can outperform the worst one 10 to 1.
* Can also try to enable the sales exec with ads and outbound, but algo's should do better

#### Facebook marketing

1. Lift testing
1. Cross device
1. Custom audience
1. Three value props times three creatives. 10x difference
1. Can't do more than five impressions of same creative
1. Vertical video. Half of it subtitles so it works with the sound off. Grab in first three seconds.
1. Drop pixel on landing page and conversion
1. Product video to broad audience
1. Solicit click with features to video viewers
1. Re target people that clicked
1. Wish does well. Staged retargeting telling a story.
1. We should measure our cac vs iavc per channel.
1. Try to correlate spend with outcomes.
1. Tell Facebook what you want: probably conversions. Machine learning with 200k inputs
1. Last week of the quarter you get outbid by brands spending money.
1. The more you spend the worse the marginal outcome.

## Cross department metrics

Cross department metrics are essential to measure our [journeys](/handbook/journeys).

## Department metrics

### Sales

1. Incremental ACV
1. ARR
1. TCV
1. ARR
1. Renewal rate
1. Win rate and time for every sale stage and the overall process.
1. Time to close
1. Pipeline created
1. Pipeline Requirement Projection
1. [Sales efficiency (above 0.8)](http://tomtunguz.com/magic-numbers/)
1. [Pipeline coverage (above 3)](https://pipetop.com/saas-glossary/sales-terms/sales-pipeline-coverage/)
1. Up-sell (EES to EEP, EEP to EEU)
1. Average deal size

### Peopleops

1. Inbound applications per week
1. Interviews per applicant
1. eNPS score of team members
1. Applicant score per requirement and value
1. Declined applicant NPS score
1. Offer accept rate
1. Accepted offers/hires
1. Cycle time (apply to accepted offer)
1. Terminations (Voluntary regrettable, Voluntary PIP, Involuntary)
1. Share of people on PIPs

### Marketing

1. Visitors to the website
1. Downloads
1. MQL
1. SQL
1. Blended CAC
1. Program spend / Total spend (above 0.5)
1. Conversion percentages, cycle time, and CAC per channel
1. New contributers from the wider community per release/month

### Engineering

1. Merge requests (total, community, ours)
1. Cycle time
1. Time to review
1. Bug fixes (cherry-picks into stable)

### Finance

1. Order to cash time
1. Runway (above 12 months)
1. [40 rule (above 40%)](http://www.feld.com/archives/2015/02/rule-40-healthy-saas-company.html)
1. [Magic number (above 0.7)](https://davidcummings.org/2009/09/21/saas-magic-number/)

### Product

1. A/B test new features
1. Money made with new features
1. Re-tweets for release tweet
1. Usage per feature
1. Usage stats
1. Bottlenecks to increased customer sophistication

Many of the metrics you would normally track outside of the product we collect inside of the product so it is also useful to our self hosted users. This includes cohort analytics, conversational development analytics, and cycle time analytics.

## Summary

* Acquire the highest LTV at the lowest CAC.
* The LTV depends on product/quantity/upsell/churn.
* The CAC depends on many factors.
* How you acquire a customer very likely influences the LTV.
* The LTV can probably be predicted quickly after purchase with lookalike customers.
You need to do bayesian logic to find out what causes people to buy.
Bayesian logic is the next generation of multi touch attribution.
Now that you know what the levers are you can automatically generate campaigns.
* You generate the campaigns using:
  * Discover.org data (largest companies)
  * Customer data (look alikes)
  * Product data (usage at companies)
  * Click data (what kind of ads they click on
* Campaigns can also involve outbound, both from SDR and AE.
* Campaigns can be customized based on product data, industry, and other factors.
* You need to continually vary your call scripts, website message, drip campaigns, ads, and decks to find out what is effective.
* Much of the variation should be automatically generated.
* Customers probably need multiple touches before purchasing, so it is important to have a sequence (broad interest, features, etc.) and to get them through this pipe.

# License

This code is distributed under the MIT license, see the [LICENSE](LICENSE) file.
