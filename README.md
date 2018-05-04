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
  * [Horizontal slice of ELT sources](https://gitlab.com/bizops/bizops/issues?scope=all&utf8=✓&state=opened&label_name[]=elt): Salesforce, Marketo, NetSuite, Zuora, etc.
  * [Data Pipeline](https://gitlab.com/bizops/bizops/issues?label_name[]=pipeline): container, CI pipeline, review apps
2. Data Model and Visualization
  * [Common Data Model](https://gitlab.com/bizops/bizops/issues?label_name[]=data-model): Conventions for common table and field names
  * [Field Mapping](https://gitlab.com/bizops/bizops/issues/121): Mapping of user fields to common data model, if required
  * [Visualization Sample](https://gitlab.com/bizops/bizops/issues/122): Documentation and samples for connecting a visualization engine
  * [JupyterHub deployment](https://gitlab.com/bizops/jupyter-hub): Easily deploy JupyterHub for data exploration
3. [Ease of use & Automation](https://gitlab.com/bizops/bizops/issues?label_name%5B%5D=ease-of-use)
  * Seamless handle some schema changes, like a field rename
  * Match user fields to common data model, without intervention

### Competition & Value

This should be a replacement for other ELT & Data Integration tools: [Boomi](https://boomi.com/), [Informatica Cloud](https://www.informatica.com/products/cloud-integration/cloud-data-integration.html), and [Alooma](https://www.alooma.com/).

## Data Engineering Lifecycle

| Stage     | BizOps Selected | OSS Considered | Proprietary Offerings |
| --------- | ------------ | -------------- | --------------------- |
| Extract   | Custom scripts | [Singer](https://www.singer.io/) Tap, [Pentaho DI](http://www.pentaho.com/product/data-integration), [Talend](https://www.talend.com/) | [Alooma](https://www.alooma.com/) |
| Load      | Custom scripts | [Singer](https://www.singer.io/) Target, [Pentaho DI](http://www.pentaho.com/product/data-integration), [Talend](https://www.talend.com/) | [Alooma](https://www.alooma.com/) |
| Transform | [dbt](https://www.getdbt.com/), Python (never stored procedures) | [Pentaho DI](http://www.pentaho.com/product/data-integration), manual SQL | [Alooma](https://www.alooma.com/) |  
| Warehouse | [PostgreSQL](https://www.postgresql.org/) | [MariaDB AX](https://mariadb.com/products/solutions/olap-database-ax) | [Redshift](https://aws.amazon.com/redshift/), [Snowflake](https://www.snowflake.net/) |
| Orchestrate | [GitLab CI](https://about.gitlab.com/features/gitlab-ci-cd/) | [Luigi](https://github.com/spotify/luigi), [Airflow](https://airflow.apache.org/) | |
| Test | [dbt](https://www.getdbt.com/), [Great Expectations](https://github.com/great-expectations/great_expectations), [Hypothesis](https://hypothesis.readthedocs.io/en/latest/) | | [Informatica](https://marketplace.informatica.com/solutions/informatica_data_validation), [iCEDQ](https://icedq.com/), [QuerySurge](http://www.querysurge.com/) |
| Explore | [JupyterHub](https://github.com/jupyterhub/jupyterhub) | [Metabase](https://www.metabase.com/) | [Nurch](https://www.nurtch.com/), [Datadog notebooks](https://www.datadoghq.com/blog/data-driven-notebooks/) |
| Modeling | Not selected yet | None found | [LookML](https://looker.com/platform/data-modeling) |
| Visualize | Not selected yet | [D3.js](https://d3js.org/) | [Looker](https://looker.com/) |

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
  * Current plans include creating separate Extractors and Loaders that can be plugged together much like Unix command line tools.
1. Transformation: [dbt](https://docs.getdbt.com/) to handle transforming the raw data into a normalized data model within PG.
1. Warehouse: Any SQL based data warehouse. We recommend [PostgeSQL](https://www.postgresql.org/) and include it in the bizops pipeline. Postgres cloud services like [Google Cloud SQL](https://cloud.google.com/sql/) are also supported, for increased scalability and durability.
1. Orchestration/Monitoring: [GitLab CI](https://about.gitlab.com/features/gitlab-ci-cd/) for scheduling, running, and monitoring the ELT jobs. In the future, [DAG](https://gitlab.com/gitlab-org/gitlab-ce/issues/41947) support will be added. Non-GitLab alternatives are [Airflow](https://airflow.incubator.apache.org) or [Luigi](https://github.com/spotify/luigi).
1. Visualization/Dashboard: BizOps is compatible with nearly all visualization engines, due to the SQL based data store. For example commercial products like [Looker](https://looker.com/) or [Tableau](https://www.tableau.com/), as well as open-source products like [Superset](https://github.com/airbnb/superset) or [Metabase](https://metabase.com) can be used.

## How to use
> Notes:
> * Most implementations of SFDC, and to a lesser degree Zuora, require custom fields. You will likely need to edit the transformations to map to your custom fields.
> * The sample Zuora python scripts have been written to support GitLab's Zuora implementation. This includes a workaround to handle some subscriptions that should have been created as a single subscription.

The BizOps product consists three key components:

1. A SQL based data store, for example [PostgreSQL](https://www.postgresql.org/) or [Cloud SQL](https://cloud.google.com/sql/). We recommend using Postgres for [review apps](https://about.gitlab.com/features/review-apps/) and a more durable and scalable service for production.
1. This project, [`bizops`](https://gitlab.com/bizops/bizops), which contains the ELT scripts and CI jobs to refresh the data warehouse from the [configured sources](doc/data_sources.md). Typically configured to run on a [scheduled CI job](https://docs.gitlab.com/ce/user/project/pipelines/schedules.html) to refresh the data warehouse from the configured sources.
1. The [`bizops-elt`](https://gitlab.com/bizops/bizops-elt) container, which includes the necessary dependencies for the ELT scripts. Used as the base image for the CI jobs.

As development progresses, additional documentation on getting started along with example configuration and CI scripts will become available.

It is expected that the BizOps project will have many applications managed in the top level of the project. Some or parts of these applications could be useful to many organizations, and some may only be useful within GitLab. We have no plans on weighing the popularity of an individual application at the top level of the BizOps project for inclusion/exclusion. 

### Local environment

The local environment might differs for differents ELT process.  
The most standard setup using Python 3.5.  

You might want to customize the `.env.example` file according to your needs.

```
$ cp .env.example > .env
```

Then, create the virtualenv using `pipenv`:
```
$ pipenv install --skip-lock
$ pipenv shell
```

This should install the python dependencies that the ELT process need.  

A docker image is also provided to start a PostgreSQL instance for the data warehousing needs.  
This image will use environment variables defined in the BizOps project.
To run:

```
$ env $(<.env) docker-compose -f stack.yml start
```

You should be ready to go!

### Managing API requests and limits

Many of the SaaS sources have various types of API limits, typically a given quota per day. If you are nearing the limit of a given source, or are iterating frequently on your repo, you may need to implement some additional measures to manage usage.

#### Reducing API usage by review apps

One of the easiest ways to reduce consumption of API calls for problematic ELT sources is to make that job manual for branches other than `master`. This way when iterating on a particular branch, this job can be manually run only if it specifically needs to be tested.

We don't want the job on `master` to be manual, so we will need to create two jobs. The best way to do this is to convert the existing job into a template, which can then be referenced so we don't duplicate most of the settings.

For example take a sample Zuora ELT job:

```yaml
zuora:
  stage: extract
  image: registry.gitlab.com/bizops/bizops-elt/extract:latest
  script:
    - set_sql_instance_name
    - setup_cloudsqlproxy
    - envsubst < "elt/config/environment.conf.template" > "elt/config/environment.conf"
    - python3 elt/zuora/zuora_export.py
    - stop_cloudsqlproxy
```

The first thing to do would to convert this into an anchor, and preface the job name with `.` so it is ignored:

```yaml
.zuora: &zuora
  stage: extract
  image: registry.gitlab.com/bizops/bizops-elt/extract:latest
  script:
    - set_sql_instance_name
    - setup_cloudsqlproxy
    - envsubst < "elt/config/environment.conf.template" > "elt/config/environment.conf"
    - python3 elt/zuora/zuora_export.py
    - stop_cloudsqlproxy
```

Next, we can define two new jobs. One for `master` and another manual job for any review branches:

```yaml
zuora_prod:
  <<: *zuora
  only:
    - master

zuora_review:
  <<: *zuora
  only:
    - branches
  except:
    - master
  when: manual
```

## GitLab Data and Analytics - Internal

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

#### Accessing the Data Warehouse
If it's determined you need direct access to the data warehouse outside of Looker or JupyterHub, these are the steps required to make that happen.

* Verify your Google account is associated with the `gitlab-analysis` project, it should have the `Cloud SQL Client` role.
* Set up your local machine with the [gcloud SDK](https://cloud.google.com/sdk/docs/).
* Run `gcloud config set project gitlab-analysis`
* Run `gcloud auth application-default login`
* Connect to cloudsqlproxy `./cloud_sql_proxy -instances=gitlab-analysis:us-west1:dev-bizops=tcp:5432`
* Connect to the Data Warehouse using your username and password on 127.0.0.1:5432 in your favorite tool.

#### Hosts Records Dataflow

From our on-premises installations, we recieve [version and ping information](https://docs.gitlab.com/ee/user/admin_area/settings/usage_statistics.html) from the software. This data is currently imported once a day from a PostgreSQL database into our enterprise data warehouse (EDW). We use this data to feed into Salesforce (SFDC) to aid our sales representatives in their work.

The domains from all of the pings are first cleaned by standardizing the URL using a package called [tldextract](https://github.com/john-kurkowski/tldextract). Each cleaned ping type is combined into a single host record. We make a best effort attempt to align the pings from the same install of the software. 

This single host record is then enriched with data from three sources: DiscoverOrg, Clearbit, and WHOIS. If DiscoverOrg has no record of the domain we then fallback to Clearbit, with WHOIS being a last resort. Each request to DiscoverOrg and Clearbit is cached in the database and is updated no more than every 30 days. The cleaning and enrichment steps are all accomplished using Python.

We then take all of the cleaned records and use dbt to make multiple transformations. The last 60 days of pings are aligned with Salesforce accounts using the account name or the account website. Based on this, tables are generated of host records to upload to SFDC. If no accounts are found, we then generate a table of accounts to create within SFDC. 

Finally, we use Python to generate SFDC accounts and to upload the host records to the appropriate SFDC account. We also generate any accounts necessary and update any SFDC accounts with DiscoverOrg and Clearbit data if any of the relevant fields are not already present in SFDC.

#### Managing Roles

All role definitions are in [/elt/config/pg_roles/](https://gitlab.com/bizops/bizops/tree/master/elt/config)

Ideally we'd be using [pgbedrock](https://github.com/Squarespace/pgbedrock) to manage users. Since internally we are using CloudSQL, we're not able to access the superuser role which pgbedrock requires. However, the YAML format of the role definitions is convenient for reasoning about privileges and it's possible the tool could evolve to validate privileges against a given spec, so we are using the pgbedrock definition syntax to define roles here. 

The `readonly` role was generated using the following commands:

```sql
CREATE ROLE readonly;

GRANT USAGE on SCHEMA analytics, customers, gitlab, license, mkto, public, sandbox, sfdc, version, zuora to readonly;

GRANT SELECT on ALL TABLES IN SCHEMA analytics, customers, gitlab, license, mkto, public, sandbox, sfdc, version, zuora to readonly;

GRANT INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER on ALL TABLES IN schema sandbox to readonly;

```

The `analytics` role was generated using the following commands:

```sql

CREATE ROLE analytics;

GRANT USAGE on SCHEMA analytics, customers, gitlab, license, mkto, public, sandbox, sfdc, version, zuora to analytics;

GRANT ALL PRIVILEGES on ALL TABLES IN SCHEMA analytics, customers, gitlab, license, mkto, public, sandbox, sfdc, version, zuora to analytics;

``` 

New user roles are added to a specific role via:

```sql
CREATE ROLE newrole WITH PASSWORD 'tmppassword' IN ROLE metarole;
```

New readonly and analytics users are then given instructions via Google Drive on how to connect their computer to the CloudSQL Proxy and on how to change their password once they login.

By default, roles cannot login to the main production instance of the data warehouse. When the role is created, an admin will change the login permission and then have the user login and change their password on the production instance. Then the user will login to the dev instance and set the password the same. The admin will then set the role on the production instance to NOLOGIN. This will ensure at the next sync that the roles are properly set. 

# Contributing to BizOps

We welcome contributions and improvements, please see the [contribution guidelines](CONTRIBUTING.md)

# License

This code is distributed under the MIT license, see the [LICENSE](LICENSE) file.
