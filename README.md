[![pipeline status](https://gitlab.com/gitlab-org/bizops/badges/master/pipeline.svg)](https://gitlab.com/gitlab-org/bizops/commits/master)

# BizOps

BizOps is a convention-over-configuration framework for analytics, business intelligence, and data science. It leverages open source software and software development best practices including version control, CI, CD, and review apps.

## Principles

We believe that information is the foundation of good decisions, and that companies of all sizes deserve insights into their operations. So BizOps provides broad, democratized access to detailed operational metrics, thereby driving better decisions and shortening decision cycle time across the entire enterprise.

We further believe that the information a business uses to make decisions must come from all parts of that business. So BizOps joins data from multiple systems used by Sales, Marketing, Product and others, thereby providing a comprehensive view of the relationship between business activities, associated costs, and customer long-term value.

## Approach

### BizOps is a product within GitLab.
For many companies GitLab serves as the single data store for their engineering organization, shepherding their ideas all the way through to delivering them to customers. There are key gaps however in understanding the effectiveness of sales and marketing. By expanding the common data store to include go to market information, additional insights can be drawn across the customer lifecycle.

### BizOps is open core and built upon open source tools.
Open core within GitLab provides the broadest possible access to core BizOps features while allowing GitLab to generate revenue with features critical to large enterprises. Building on an open source toolset also provides collaboration opportunities to improve tooling, establish best practices, and move more quickly.

### BizOps is BI as code.
BizOps uses GitLab CI/CD to setup and maintain its stack, so software and scripts required are checked into SCM with the attendant benefits: full version control, history, easy collaboration and reviews. Automated management of the BI environment means it is easy to make alterations, re-deploy in the event of an issue or outage, as well as provision new environments for different uses like a staging server.

### BizOps works for GitLab first.
We are building BizOps to solve a problem that we share with all other software companies - how to acquire the highest-value customers at the lowest cost of acquisition?  We are solving this problem for ourselves first, incorporating what we learn along the way into a product that delivers practical and quantifiable value to our customers.

## Roadmap

1. MVC
  * Horizontal slice of ELT sources: Salesforce, Marketo, NetSuite, Zuora
  * Data Pipeline: container, CI pipeline, review apps
2. Data Model and Visualization
  * Common Data Model: Conventions for common table and field names
  * Field Mapping: Mapping of user fields to common data model, if required
  * Visualization Sample: Documentation and samples for connecting a visualization engine
3. Ease of use & Automation
  * Seamless handle some schema changes, like a field rename
  * Match user fields to common data model, without intervention

### Competition & Value

This should be a replacement for other ELT & Data Integration tools: [Boomi](https://boomi.com/), [Informatica Cloud](https://www.informatica.com/products/cloud-integration/cloud-data-integration.html), and [Alooma](https://www.alooma.com/).

## Data Engineering Lifecycle

| Stage     | BizOps Selected | OSS Considered | Proprietary Offerings |
| --------- | ------------ | -------------- | --------------------- |
| Extract   | [Singer](https://www.singer.io/) Tap | [Pentaho DI](http://www.pentaho.com/product/data-integration), [Talend](https://www.talend.com/) | [Alooma](https://www.alooma.com/) |
| Load      | [Singer](https://www.singer.io/) Target | [Pentaho DI](http://www.pentaho.com/product/data-integration), [Talend](https://www.talend.com/) | [Alooma](https://www.alooma.com/) |
| Transform | [dbt](https://www.getdbt.com/) | [Pentaho DI](http://www.pentaho.com/product/data-integration), manual SQL | [Alooma](https://www.alooma.com/) |  
| Warehouse | [PostgreSQL](https://www.postgresql.org/) | [MariaDB AX](https://mariadb.com/products/solutions/olap-database-ax) | [Redshift](https://aws.amazon.com/redshift/), [Snowflake](https://www.snowflake.net/) |
| Orchestrate | [GitLab CI](https://about.gitlab.com/features/gitlab-ci-cd/) | [Luigi](https://github.com/spotify/luigi), [Airflow](https://airflow.apache.org/) | |
| Test | [dbt](https://www.getdbt.com/), [Great Expectations](https://github.com/great-expectations/great_expectations), [Hypothesis](https://hypothesis.readthedocs.io/en/latest/) | | [Informatica](https://marketplace.informatica.com/solutions/informatica_data_validation), [iCEDQ](https://icedq.com/), [QuerySurge](http://www.querysurge.com/) |
| Model | Out of scope | | |
| Visualize | Out of scope | | |

## Metrics

We are targeting analytics for sales and marketing performance first. We plan to track the following metrics, in order of priority. These results will be able to reviewed over various time periods. Initially we will support single touch attribution, with support for multitouch in a [later sprint](doc/development_plan.md#backlog).

1. SAOs by source
  1. Aggregated (SDR / BDR / AE generated / Other)
  1. Campaign level (AWS Reinvent / etc.)
1. SAOs by source by week and/or month
2. Aquisition cost per SAO
  * Cost per lead = dollar spend / number of attributed leads
3. Estimated IACV and LTV per SAO based on history (can do IACV if LTV is hard to calculate)
  * Estimated IACV = 2 *  IACV at median conversion time
  * LTV = IACV * margin * average retention time
4. Estimated IACV / marketing ratio.
  * CAC = cost per lead * conversion from lead to IACV
  * ROI = LTV / CAC

In the future, we plan to expand support to other areas of an organization like Customer Success, Human Resources, and Finance.

## Data sources

To achieve this, we bring data from all [data sources](data_sources.md) to a [common data model](doc/data_model.md) so it can be used easily and consistently across tools and teams. For example something as simple as unique customer ID, product or feature names/codes.

## Tools

We want the tools to be open source so we can ship this as a product.

1. Extract and Load (EL): Combination of [Pentaho Data Integration](http://www.pentaho.com/product/data-integration) and python scripts, although will consider [Singer](https://www.singer.io/) once it supports Salesforce and PostgreSQL.
  * Pentaho DI is based on the open-source [Talend](https://www.talend.com/products/data-integration/) engine, but utilizes XML for easier configuration.
1. Transformation: [dbt](https://docs.getdbt.com/) to handle transforming the raw data into a normalized data model within PG.
1. Warehouse: Any SQL based data warehouse. We recommend [PostgeSQL](https://www.postgresql.org/) and include it in the bizops pipeline. Postgres cloud services like [Google Cloud SQL](https://cloud.google.com/sql/) are also supported, for increased scalability and durability.
1. Orchestration/Monitoring: [GitLab CI](https://about.gitlab.com/features/gitlab-ci-cd/) for scheduling, running, and monitoring the ELT jobs. Non-GitLab alternatives are [Airflow](https://airflow.incubator.apache.org) or [Luigi](https://github.com/spotify/luigi).
1. Visualization/Dashboard: BizOps is compatible with nearly all visualization engines, due to the SQL based data store. For example commercial products like [Looker](https://looker.com/) or [Tableau](https://www.tableau.com/), as well as open-source products like [Superset](https://github.com/airbnb/superset) or [Metabase](https://metabase.com) can be used.

## How to use

The BizOps product consists three key components:

1. A SQL based data store, for example [PostgreSQL](https://www.postgresql.org/) or [Cloud SQL](https://cloud.google.com/sql/). We recommend using Postgres for [review apps](https://about.gitlab.com/features/review-apps/) and a more durable and scalable service for production.
1. This [`bizops`](#extract-container), which contains the ELT scripts and CI jobs to refresh the data warehouse from the [configured sources](doc/data_sources.md). Typically configured to run on a [scheduled CI job](https://docs.gitlab.com/ce/user/project/pipelines/schedules.html) to refresh the data warehouse from the configured sources.
1. The [`bizops-elt`](https://gitlab.com/bizops/bizops-elt) container, which includes the necessary dependencies for the ELT scripts. Used as the base image for the CI jobs.

As development progresses, additional documentation on getting started along with example configuration and CI scripts will become available.

It is expected that the BizOps project will have many applications managed in the top level of the project. Some or parts of these applications could be useful to many organizations, and some may only be useful within GitLab. We have no plans on weighing the popularity of an individual application at the top level of the BizOps project for inclusion/exclusion.  

## Internal GitLab Analytics Plan

### Charter/Goals
* Build a centralized data warehouse that can support data analysis requirements from all functional groups within the company.
* Create a common data framework and governance practice.
* Establish the single source of truth for company metrics.
* Establish a change management processes for source systems.
* Develop a Data Architecture plan (in conjunction with functional teams).
* Develop a roadmap for systems evolution in alignment with the Company’s data architecture plan.

### GitLab Internal Analytics Architecture

![GitLab Internal Analytics Architecture](img/WIP_ GitLab_Analytics_Architecture.jpg)

#### Staging Tables

* We’ll want to stage our data before loading it into the data warehouse.
* Local Postgres db's are a good choice if we are not using Cloud SQL.
* Primarily used for transformation and data scrubbing prior to loading into the Data Warehouse.
* Allows for data quality monitoring of source data.
* Minimizes impact to production systems.
* Ideally incremental loads (extract only what changed since the last extract).
* Prevents need to query production db's impacting app performance

#### Data Warehouse

* Using Cloud SQL.
* Consolidated repository of all source data - scrubbed and modeled into a format optimized for analytic workliads (Dimensional model).
* Serves as the Single Source of Truth for reporting, analysis, and visualization applications.
* Will need to be audited regularly back to the source.
* Should not be generally available - will require strict access controls for direct querying not done through a controlled application such as metabase.

# Contributing to BizOps

We welcome contributions and improvements, please see the [contribution guidelines](CONTRIBUTING.md)

# License

This code is distributed under the MIT license, see the [LICENSE](LICENSE) file.
