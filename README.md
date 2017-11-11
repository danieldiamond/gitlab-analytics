# Bizops

BizOps is a convention-over-configuration framework for analytics, business intelligence, and data science. It leverages open source software and software development best practices including version control, CI, CD, and review apps

## Objectives

1. Provide an open source analytics and BI tool to analyze sales and marketing performance ([in progress](#development-plan))
1. Expand into customer success insight and automation
1. Close the feedback loop between product feature launches and sales/marketing performance
1. Widen to include other areas of the company, like people ops and finance

## Development Plan

For the MVP of BizOps, we plan to delivering the following based on [Objective #1](#objectives): 

* A configurable ELT engine to retrieve data out of SFDC, Zuora, and Marketo
* A BI dashboard to view the ELT'd data
* Sample dashboards to get started

This would provide a basic foundation for analyzing your CRM data that is contained within SFDC.

### Sprints

#### Priority 1

For the very first MVC, we should focus on just getting an environment established which can ELT and render data: 
* [Create a container with Talend and dbt, to be used as image for CI job](https://gitlab.com/gitlab-org/bizops/issues/8) (Have VM today)
  * Starts up, uses ENV vars to auth to SFDC/Zuora/Marketo, ELT's data into PG. Runs dbt to transform to data model.
* Create a container with PG and Superset (Done)
  * Is the "app" that runs as the environment 
* Rely on the end user for the "extract" (App -> PG) transformation files (our version is WIP)
* Establish [standard data model](https://gitlab.com/gitlab-org/bizops/issues/9) for required fields
* Rely on the end user for "transform" (staging->data model) transformation (not yet started)
* Create the initial dashboard views based on standard model (our version not yet started)

#### Priority 2

Automate & provide guide rails for ELT phase
* Create script to grab SFDC objects to create a transformation KTL file automatically (to load data into staging tables)
* Create script to check user provided mapping file for required fields (staging field -> data model field), list missing ones

#### Priority 3

Make working with data easier

* Copy dashboards from the repo into Superset, to provide OOTB templates
* Identify an easy "flow" to save modified dashboard into repo. (Cut/Paste, download file, etc.)

#### Backlog

* Productize this a little more, and add steps to ease the creation of the "transform" file.
* Set up backup/restore jobs for production database

#### Open questions

* Should we required Marketo/Zuora data to be in SFDC, or pull from these platforms directly?
  * Pulling only from SFDC would generalize the process if customers used other tools, but then require that the integration and data is written back to SFDC

## Metrics

These are the metrics we plan to track from the sales and marketing data, per campaign:

* Cost per lead = dollar spend / number of attributed leads
* Median conversion time (from touch to IACV)
* IACV at median conversion time (at for example 90 days after touch)
* Estimated IACV = 2 *  IACV at median conversion time
* CAC = cost per lead * conversion from lead to IACV
* LTV = IACV * margin * average retention time
* ROI = LTV / CAC
* Collateral usage

For each campaign, we should also be able to review over various time periods, and per lead/organization.

## User Experience

### Admin Flow

1. Start a new project with the BizOps template.
1. Add salesforce app credentials as environment variables.
1. Redeploy and look at a link that shows salesforce metadata. (can we make redeploy something that happens after setting environmental variables)
1. Use metadata to populate transform.yml and commit to master
1. Redeploy happens automatically and you see a insightsquared like graph.
  1. ELT runs and outputs into PG. 
  1. Superset is then with PG as data source, with a set of dashboards loaded from a file in the repo.
  1. URL is set to be location of Superset 

### User Flow

1. Measure the relationship between audience, content, and customers.
1. Generate variations on audience and content.
1. Find out if these lower CAC or produce a higher LTV.
1. Move your spend to the ones that perform better.
1. Repeat.

### Interaction Flow

Both marketing, sales, and customer success people are shown:
* Show the CAC and LTV of cohort/account/lead, what it is based on (how much / when) and the history of touchpoints.
* Actions (automatically scheduled ones, possible manual ones) with the calculated lift in LTV of each.

New actions can be programmed:
* Can be ads, an outbound message
* Can be triggered manually or automatic
* Can be dependent on doing something else first (need to watch video first, etc.)

## Data sources

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

### Data Flow

The general data flow would be SFDC->Talend->PG->Superset:
1. SFDC credentials are available to Talend via environment variables
1. A transformation file is in the repo, which describes which columns to retrieve and how to write them into PG
1. Talend is executed to ELT the data into PG
1. A Superset instance is spawned, connected to the PG DB
1. The dashboards for Superset are stored in the repo

### Table types

* Staging (raw import)
* Fact (sales lines), gets really long
* Dimensions (slice, for examples stages), can be slowly changing with time stamps, historical role
* Surrogate keys (integer that links a fact to a dimension) to store less bytes

## Variables

### Audience variables

1. Tracking data
  1. Visits to website
  1. Clicks on links
  1. Clicks on ads
1. Product data, for example for GitLab
  1. GitLab.com
  1. Version.gitlab.com
  1. Issue tracker of GitLab.com comments
1. Customer data (are they a customer, did they use to be, on a trial)
1. Lookalike data (do they match a custom audience)
1. CRM data (Salesforce)
1. Support data (Zendesk)
1. General metrics (age, etc.)
1. Interests (like DevOps, etc.)

### Content variables

* Message (benefit, feature)
* Medium (video, graphic, text)
* Creative (black and white, in your face)
* Platform (Facebook, Google, LinkedIn, other)
* Surface (Messenger, Whatsapp, Instagram)

### Customer variables

* Product (what do they buy)
* Quantity (how much do they buy)
* Upsell (how much do they expand)
* Churn (how long do they stay a customer)

### Marketing variables

* Paid content (ads, sem)
* Owned content marketing (website, blog, email, seo)
* Earned content (social media shares, PR, AR)
* DevRel marketing (meetups, conferences)
* Event marketing (trade fairs)

## Tools

We want the tools to be open source so we can ship this as a product. 

1. Extract and Load (EL): [Talend](Talend Real-Time Open Source Big Data Integration Software) for the ELT engine, although are considering [Singer](https://www.singer.io/) once it supports Salesforce and PostgreSQL.
1. Transformation: [dbt](https://docs.getdbt.com/) to handle transforming the raw data into a normalized data model within PG.
1. Warehouse: [PostgeSQL](https://www.postgresql.org/), maybe later with [a column extension](https://github.com/citusdata/cstore_fdw). If people need SaaS BigQuery is nice option and [Druid](http://druid.io/) seems like a good pure column oriented database.
1. Display/analytics: [Superset](https://github.com/airbnb/superset) (Apache open source, [most](https://github.com/apache/incubator-superset/pulse/monthly) [changed](https://github.com/metabase/metabase/pulse/monthly) [repository](https://github.com/getredash/redash/pulse/monthly)) instead of the open source alternative [Metabase](https://github.com/metabase/metabase) which is Clojure based and runs on the JVM or [Redash](https://redash.io/). Proprietary alternates are Looker and Tableau.
1. Orchestration/Monitoring: [GitLab CI](https://about.gitlab.com/features/gitlab-ci-cd/) for scheduling, running, and monitoring the ELT jobs. Non-GitLab alternatives are [Airflow](https://airflow.incubator.apache.org) or [Luigi](https://github.com/spotify/luigi) s.

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