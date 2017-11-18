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

1. Extract and Load (EL): Combination of [Pentaho Data Integration](http://www.pentaho.com/product/data-integration) and python scripts, although are considering [Singer](https://www.singer.io/) once it supports Salesforce and PostgreSQL.
  * Pentaho DI is based on the open-source [Talend](https://www.talend.com/products/data-integration/) engine, but utilizes XML for easier confiugration.
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
* [Conway's law](https://en.wikipedia.org/wiki/Conway%27s_law), in summary: "organizations are constrained to produce designs which copy their communication structures"
* Based on open source tools, 100k organizations should create it together based on mutual best practices
* Democratize access to business performance, drive better decisions across the org
* All companies deserve have the business insights of a F500 org, investors will insist on this
* The companies of the future will build a detailed, actionable model of each individual customer

There are many [additional concepts and opportunities](doc/concepts.md) to utilize this data internally and externally.

## Why integrated

* Determining critical data like CAC and LTV depends on many touch points across many systems
* Product should inform sales efforts, with a sales and marketing insights feedback into SDLC
* Good products need to know more about their users and their needs, this requires a comprehensive data warehouse tracking all touch points
* Ultimate goal of fusing product data with sales and marketing data, to deliver more impactful, actionable insights across the lifecycle

### Examples

The product can also harness deep insights about specific customers to improve UX and LTV:

* Show financials an ad about compliance.
* If product shows someone just started to use CI show ads with CI information.
* Add logo's from the same industry to an auto generated presentation
* Tie campaigns to software milestones, to determine sales & marketing lift per release
* Integrate your marketing operations with product features, integrating your drip campaign with recent actions taken / untaken

## Competition & Value

This should be a replacement for:
* Marketing automation (Marketo)
* Sales analytics (Insightsquared)
* Customer Success automation (Gainsight)

You should also be able to retire:
* Analytics (Tableau)
* Sales content management (Highspot/Docsend)

In the beginning the software should build on some existing systems, for example the drip campaigns in Marketo.

## Summary

* Acquire the highest LTV at the lowest CAC
* Drive better, data driven decisions across all sectors of the organization
* The LTV can likely be predicted quickly after purchase with lookalike customers
* Customers need multiple touches before purchasing, so it is important to have a sequence (broad interest, features, outbound, etc.) and to get them through this pipe.
* You need to continually vary your call scripts, website message, drip campaigns, ads, and decks to find out what is effective. Customized for each user and their journey with your company.
* Determine what causes people to buy and grow. Campaigns and product nudges need to align with these levers.

# Contributing to BizOps

We welcome contributions and improvements. The source repo for our Helm Charts can be found here: <https://gitlab.com/gitlab-org/bizops>

Please see the [contribution guidelines](CONTRIBUTING.md)

# License

This code is distributed under the MIT license, see the [LICENSE](LICENSE) file.
