## Bizops


Finance has the company wide responsibility for analytics.
Examples of the types of metrics we want to track are below per department.

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

### Data sources

1. Salesforce
1. Zuora
1. Marketo
1. Zendesk
1. NetSuite
1. Mailchimp
1. Google Analytics
1. Discover.org
1. Clearbit
1. Lever
1. GitLab version check
1. GitLab usage ping
1. [GitLab.com](https://about.gitlab.com/handbook/engineering/workflow/#getting-data-about-gitlabcom)

We want a single data warehouse and a central data model. We bring all relevant data to single storage place. Make it platform agnostic, so for example do not build it into Salesforce. We want to build data model to be used consistently across tools and teams. For example something as simple as unique customer ID, product or feature names/codes.

On the other hand we're open to pragmatic solutions linking for example Salesforce and Zendesk, if there are boring solutions available we'll adopt them instead of creating our own.

### Tools

We want the tools to be open source so we can ship this as a product. Also see the "Complete BizOps" Google Doc. We'll use Singer, PostgreSQL, and Superset.

1. Ingestion/ETL: [Talend](Talend Real-Time Open Source Big Data Integration Software) for the ETL engine, along with [dbt](https://docs.getdbt.com/).
1. Warehouse: [PostgeSQL](https://www.postgresql.org/), maybe later with [a column extension](https://github.com/citusdata/cstore_fdw). If people need SaaS BigQuery is nice option and [Druid](http://druid.io/) seems like a good pure column oriented database.
1. Display/analytics: [Superset](https://github.com/airbnb/superset) (Apache open source, [most](https://github.com/apache/incubator-superset/pulse/monthly) [changed](https://github.com/metabase/metabase/pulse/monthly) [repository](https://github.com/getredash/redash/pulse/monthly)) instead of the open source alternative [Metabase](https://github.com/metabase/metabase) which is Clojure based and runs on the JVM or [Redash](https://redash.io/). Proprietary alternates are Looker and Tableau.

## MVP Proposal

For the first iteration of BizOps, we would like to ship the following:

* A configurable ETL engine to retrieve data out of SFDC
* A BI dashboard to view the ETL'd data
* Sample dashboards to get started

This would provide a basic foundation for analyzing your CRM data that is contained within SFDC.

### User Flow

1. Start a new project with the BizOps template.
1. Add salesforce app credentials as environment variables.
1. Redeploy and look at a link that shows salesforce metadata. (can we make redeploy something that happens after setting environmental variables)
1. Use metadata to populate transform.yml and commit to master
1. Redeploy happens automatically and you see a insightsquared like graph.
  1. ETL runs and outputs into PG. 
  1. Superset is then with PG as data source, with a set of dashboards loaded from a file in the repo.
  1. URL is set to be location of Superset 

### Data Flow

The general data flow would be SFDC->Talend->PG->Superset:
1. SFDC credentials are available to Talend via environment variables
1. A transformation file is in the repo, which describes which columns to retrieve and how to write them into PG
1. Talend is executed to ETL the data into PG
1. A Superset instance is spawned, connected to the PG DB
1. The dashboards for Superset are stored in the repo

### Phases

#### Phase 1

For the very first MVC, we should focus on just getting an environment established which can ETL and render data: 
* Create a container with Talend, to be used as image for CI job
  * Starts up, uses ENV vars to auth to SFDC, ETL's data into PG
* Create a container with PG and Superset
  * Is the "app" that runs as the environment 
* Rely on the end user for the transformation files
* Rely on the end user for the dashboard files

#### Phase 2

Make it more usable for GitLab data teams: 
* Copy dashboards from the repo into Superset, to provide OOTB templates
* Identify an easy "flow" to save modified dashboard into repo. (Cut/Paste, download file, etc.)

#### Phase 3

* Productize this a little more, and add steps to ease the creation of the transformation file.
* Set up backup/restore jobs for production database

## How to use

### Dockerfile
The image combines [Apache Superset](https://superset.incubator.apache.org/index.html) with a [PostgreSQL](https://www.postgresql.org/) database. It creates an image pre-configured to use a local PostgreSQL database and loads the sample data and reports. The setup.py file launches the database as well as starts the Superset service on port 8080 using [Gunicorn](http://gunicorn.org/).


### To launch the container:

1. Clone the repo and cd into it.
2. Edit the config/bizops.conf file to enter in a username, first name, last name, email and password for the Superset administrator.
3. Optionally, you can edit the user and password in the Dockerfile on line 35 to change the defaule postgres user/password. If you do this, you'll also need to update the SQLAlchemy url in the /config/superset_config.py file on line 21
4. Build the image with `docker build --rm=true -t bizops .`.
5. Run the image with `docker run -p 80:8088 bizops`.
6. Go to [http://localhost](http://localhost) and log in using the credentials you entered in step 2. 


### License

This code is distributed under the MIT license, see the [LICENSE](LICENSE) file.