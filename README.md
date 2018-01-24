[![pipeline status](https://gitlab.com/gitlab-org/bizops/badges/master/pipeline.svg)](https://gitlab.com/gitlab-org/bizops/commits/master)

# BizOps

BizOps is a convention-over-configuration framework for analytics, business intelligence, and data science. It leverages open source software and software development best practices including version control, CI, CD, and review apps.

## Premise

We believe that information is the foundation of good decisions, and that companies of all sizes deserve insights into their operations.  BizOps provides broad, democratized access to detailed operational metrics, thereby driving better decisions and shortening decision cyle time across the entire enterprise.

## Approach

BizOps is a product within GitLab.  
* For many companies GitLab serves as the single data store for their engineering organization, shepherding their ideas all the way through to delivering them to customers.  There are key gaps however in understanding the effectiveness of sales and marketing. By expanding the common data store to include go to market information, additional insights can be drawn across the customer lifecycle.

BizOps is open core, and built upon open source tools.
* Open core within GitLab provides the broadest possible access to core BizOps features while allowing GitLab to generate revenue with features critical to large enterprises.  Building on an open source toolset also provides collaboration opportunities to improve tooling, establish best practices, and move more quickly.

BizOps is BI as code.
* BizOps uses GitLab CI/CD to setup and maintain its stack, so software and scripts required are checked into SCM with the attendant benefits: full version control, history, easy collaboration and reviews.
* Automated management of the BI environment means it is easy to make alterations, re-deploy in the event of an issue or outage, as well as provision new environments for different uses like a staging server.


### Why integrated

* Determining critical data like CAC and LTV depends on many touch points across many systems
* Good products need to know more about their users and their needs, requiring comprehensive data warehouse tracking all touch points
* Product usage should inform sales efforts, and sales and marketing insights need to feed back into the planning process
* Ultimate goal of fusing product data with sales and marketing data, to deliver more impactful, actionable insights across the lifecycle

### Competition & Value

This should be a replacement for:
* Sales analytics (Insightsquared)
* Customer Success visibility (Gainsight)
* Analytics (Tableau)

In the beginning the software should build on some existing systems, for example the drip campaigns in Marketo.

### Summary

* Acquire the highest LTV at the lowest CAC
* Drive better, data driven decisions across all sectors of the organization
* The LTV can likely be predicted quickly after purchase with lookalike customers
* Customers need multiple touches before purchasing, so it is important to have a sequence (broad interest, features, outbound, etc.) and to get them through this pipe.
* You need to continually vary your call scripts, website message, drip campaigns, ads, and decks to find out what is effective. Customized for each user and their journey with your company.
* Determine what causes people to buy and grow. Campaigns and product nudges need to align with these levers.

## Objectives

### Roadmap

To address these needs, the roadmap for BizOps is:

1. Build an open source BI product to analyze sales and marketing performance ([in progress](#development-plan))
1. Expand into customer success insight and automation
1. Close the feedback loop between product feature launches and sales/marketing performance
1. Widen to include other areas of the company, like people ops and finance

### Maturity

#### Alpha
 * Open source project
   * Dedicated non-gitlab.com TLD to decouple DNS and other things
   * Self-signed TLS certs, or self-purchased
   * Project folder in official gitlab GCP account (desired by Google)
   * Non-automated infrastructure (manual patching, etc.)
   * Open source repo, contributions
   * Advice from security leader
   * Advice from infrastructure team
   * No constraints on tearing down infrastructure or losing data
   * No requirements for central monitoring of infrastructure
   * Cloud-specific tuning (ie leveraging GCP Cloud SQL/BigQuery)
 * Internal usage
   * Data warehouse and configurations backed up
   * OAuth2 Proxy used for authentication with Google accounts

#### Beta

* tbd.gitlab.com subdomain
* Gitlab infra supplied SSL certificates
* Monitoring of critical systems by GitLab Infrastructure team 
* Infrastructure automation: Docker, k8s helm
* Champions in targeted customer organization
* Data backups
* No marketing
* Cloud-agnostic

#### GA

* Sales can sell unlimited units without checking with PM or Engineering
* No data loss
* All components monitored by Infra team
* Product marketing: pricing packaging


## Development Status

BizOps is pre-alpha and under active development, focused on delivering [objective #1](#objectives). More information and current status is available in our [development plan](doc/development_plan.md).

## Analyzing Sales & Marketing Performance

As part of [objective #1](#objectives), we are targeting analytics for sales and marketing performance. We plan to track the following metrics per campaign, lead, and organization. These results will be able to reviewed over various time periods. Initially we will support single touch attribution, with support for multitouch in a [later sprint](doc/development_plan.md#backlog).

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

1. Extract and Load (EL): Combination of [Pentaho Data Integration](http://www.pentaho.com/product/data-integration) and python scripts, although will consider [Singer](https://www.singer.io/) once it supports Salesforce and PostgreSQL.
  * Pentaho DI is based on the open-source [Talend](https://www.talend.com/products/data-integration/) engine, but utilizes XML for easier configuration.
1. Transformation: [dbt](https://docs.getdbt.com/) to handle transforming the raw data into a normalized data model within PG.
1. Warehouse: [PostgeSQL](https://www.postgresql.org/), maybe later with [a column extension](https://github.com/citusdata/cstore_fdw). If people need SaaS BigQuery is nice option and [Druid](http://druid.io/) seems like a good pure column oriented database.
  * Some data (e.g. Slowly Changing Dimensions or pipeline status history) will be peristed with a cloud provider, while the rest will reside in the [`bizops`](#bizops-container) container.  Data chosen for persistence should not require modification as the app evolves and should be consumable by feature branches as well as production.
1. Display/analytics: [Metabase](https://metabase.com) to visualize the [metrics](#metrics). We evaluated [Superset](https://github.com/airbnb/superset), however it's single table limitation proved too limiting.
1. Orchestration/Monitoring: [GitLab CI](https://about.gitlab.com/features/gitlab-ci-cd/) for scheduling, running, and monitoring the ELT jobs. Non-GitLab alternatives are [Airflow](https://airflow.incubator.apache.org) or [Luigi](https://github.com/spotify/luigi).

## How to use

We will be delivering two containers to power BizOps:
1. The [`bizops`](#bizops-container) container, which houses the data warehouse and analytics software.
1. The [`extract`](#extract-container) container, which will run on a [scheduled CI job](https://docs.gitlab.com/ce/user/project/pipelines/schedules.html) to refresh the data warehouse from the configured sources.

As development progresses, additional documentation on getting started along with example configuration and CI scripts will become available.

### BizOps container
> Note this will be updated with Metabase in the near future. See [Tools](#tools) for more information.

The `bizops` image combines [Apache Superset](https://superset.incubator.apache.org/index.html) with a [PostgreSQL](https://www.postgresql.org/) database. It creates an image pre-configured to use a local PostgreSQL database and loads the sample data and reports. The setup.py file launches the database as well as starts the Superset service on port 8080 using [Gunicorn](http://gunicorn.org/).

#### Using the BizOps container

1. Clone the repo and cd into it.
2. Edit the config/bizops.conf file to enter in a username, first name, last name, email and password for the Superset administrator.
3. Optionally, you can edit the user and password in the Dockerfile on line 35 to change the defaule postgres user/password. If you do this, you'll also need to update the SQLAlchemy url in the /config/superset_config.py file on line 21
4. Build the image with `docker build --rm=true -t bizops .`.
5. Run the image with `docker run -p 80:8088 bizops`.
6. Go to [http://localhost](http://localhost) and log in using the credentials you entered in step 2.

### Extract container

The `extract` image includes:
* Pentaho Data Integration 7.1 with OpenJDK 8 to extract data from SFDC
* Python 2.7.13 and extraction scripts for Zuora and Marketo

This image is set up to be able to run periodically to connect to the configured [data sources](doc/data_sources.md) and extract data, processing it and storing it in the data warehouse running using the [`bizops container`](#bizops-container). Supported sources in current version:
* SFDC
* Zuora

#### Using the Extract container
> Notes:
> * Most implementations of SFDC, and to a lesser degree Zuora, require custom fields. You will likely need to edit the transformations to map to your custom fields. This will be automated in [Sprint 2](doc/development_plan.md#sprint-2).
> * The sample Zuora python scripts have been written to support GitLab's Zuora implementation. This includes a workaround to handle some subscriptions that should have been created as a single subscription.

The container is primarily built to be used in conjunction with GitLab CI, to automate and schedule the extraction of data. Creating the container is currently a manual job, since it changes infrequently and consumes network/compute resources. To build the container initially or after changes, simply run the `extract_container` job in the `build` stage. The subsequent `extract` stage can be cancelled and restarted once the container has finished building. This will be improved in the future.

Together with the `.gitlab-ci.yml` file and [project variables](https://docs.gitlab.com/ce/ci/variables/README.html#protected-secret-variables), it is easy to configure. Simply set the following variables in your project ensure that the container is available.
* PG_ADDRESS: IP/DNS of the Postgres server.
* PG_PORT: Port number of the Postgres server, typically 5432.
* PG_DATABASE: Database name to be used for the staging tables.
* PG_DEV_SCHEMA: Schema to use for development of dbt models.
* PG_PROD_SCHEMA: Schema to use for production dimensional model. 
* PG_USERNAME: Username for authentication to Postgres.
* PG_PASSWORD: Password for authentication to Postgres.
* SFDC_URL: Web service URL for your SFDC account.
* SFDC_USERNAME: Username for authentication to SFDC.
* SFDC_PASSWORD: Password for authentication to SFDC.
* ZUORA_URL: Web service URL for your Zuora account.
* ZUORA_USERNAME: Username for authentication to Zuora.
* ZUORA_PASSWORD: Password for authentication to Zuora.

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

#### ETL Layer
* Since we are using Open Source tools, Pentaho DI is the industry standard when they have OOB connectors (SFDC is the primary need).
* Java based - Will need to install their engine on a VM to process ETL jobs.
* Where OOB connectors for Pentaho do not exist, Python scripts can be used to create the Extraction jobs. 
* For the transformation layer needed to create the analytic data model, DBT is a good candidate. Otherwise, we’ll use standard SQL in the postgres db to accomplish the transformations. DBT is preferred as it can be version controlled through YAML files more easily than standard SQL. 

#### Staging Tables
* We’ll want to stage our data before loading it into the data warehouse. 
* Postgres dbs are a good choice if we are not using BigQuery. 
* Primarily used for transformation and data scrubbing prior to loading into the Data Warehouse.
* Allows for data quality monitoring of source data.
* Minimizes impact to production systems.
* Ideally incremental loads (extract only what changed since the last extract). 
* Prevents need to query production dbs impacting app performance

#### Data Warehouse

* Using GCP VMs with Postgres, will likely need to move to BigQuery in the future.
* Consolidated repository of all source data - scrubbed and modeled into a format optimized for analytic workliads (Dimensional model).
* Serves as the Single Source of Truth for reporting, analysis, and visualization applications.
* Will need to be audited regularly back to the source.
* Should not be generally available - will require strict access controls for direct querying not done through a controlled application such as metabase. 

#### Application Server

* Used for ad-hoc and scheduled offline processes
* Python scripting for data transformation or model calculations
* Potentially can be used as an R server in the future
* Can be used to serve custom Highcharts or Dashing apps - must be behind Authorization. 
* VM in the Data Warehouse environment

#### Data Visualization

* We’ll start with Metabase to comply with Open Source requirements. If it does not meet our needs, we can look at proprietary tools.
* Consolidated, automatically updated, near real-time reporting on a single consistent and audited data set.
* Allows for ad-hoc data exploration to identify trends and anomalies.
* Executive and management dashboards for KPI updates at a glance.
* Allows for drilling deeper into data to identify targeted business opportunities.
* Allows us to spend more time discussing metrics that are generally available as opposed to giving updates in meetings. 

### Major initiatives/Priority of Execution

**Now**

1. Finish foundational work - get metabase up with proper security
2. SAO by source and by campaign
3. CAC by channel

**Next** 

* Automate and create visualizations of the [GitLab metrics sheet](https://docs.google.com/spreadsheets/d/1-HjIWMwJZ9nUxc9XKXIIps3pgR_9VyocpG7YN0dCVZ4/edit#gid=692213658).  Need to further differentiate priority within this set.
* All of InsightSquared
* All metrics that are in OKRs visualized
* Usage data

# Contributing to BizOps

We welcome contributions and improvements, please see the [contribution guidelines](CONTRIBUTING.md)

# License

This code is distributed under the MIT license, see the [LICENSE](LICENSE) file.
