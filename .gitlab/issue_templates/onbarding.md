## Tools & Access

Goal: To help bring you, our new data analyst, up to speed in the GitLab data team's analytics stack as efficiently as possible, without sacrificing quality for speed. 


- [ ] Manager: Add to Analytics API user.
- [ ] Manager: Add to Stitch. 
- [ ] Manager: Add Developer access to Looker.
- [ ] Manager: Add to Snowflake [following Readme Process](https://gitlab.com/meltano/analytics#managing-roles-for-snowflake). 

WELCOME TO THE TEAM. WE'RE SO EXCITED TO HAVE YOU. 

- [ ] Read the following pages of the handbook in their entirety. 
   - [ ] [Data & Analytics](https://about.gitlab.com/handbook/business-ops/data-and-analytics/index.html)
   - [ ] [Business Operations](https://about.gitlab.com/handbook/business-ops/)
   - [ ] [Data Quality Process](https://about.gitlab.com/handbook/business-ops/data-quality-process/)
- [ ] Watch @tlapiana's [talk at DataEngConf](https://www.youtube.com/watch?v=eu623QBwakc) that gives a phenomenal overview of how the team works.

Our data stack looks roughly like this:
<img src = "https://cdn-images-1.medium.com/max/2000/1*BogoeTTK1OXFU1hPfUyCFw.png">

As you read in the handbook, we currently use Stitch and Meltano for extracting data from its raw sources and loading it into our Snowflake data warehouse. We use open source dbt (more on this in a moment) as our transformation tool and Looker as our business intelligence/data visualization tool. The bulk of your projects and tasks will be in dbt and Looker, so we will spend a lot of time familiarizing yourself with those tools and then dig into specific data sources. 

- [ ] Download a SQL development tool that is compatible with Snowflake, such as [SQLWorkbench/J](http://sql-workbench.net) or [DataGrip](https://www.jetbrains.com/datagrip/). If you're interested in DataGrip, follow the [instructions to get a JetBrains license in the handbook](https://about.gitlab.com/handbook/tools-and-tips/#jetbrains). Snowflake also has a Web UI that some people prefer, to each their own.

## dbt

- [ ] Familiarize yourself with [dbt](https://www.getdbt.com/), which we use for transformations in the data warehouse, as it gives us a way to source control the SQL. 
- [ ] [This article](https://blog.fishtownanalytics.com/what-exactly-is-dbt-47ba57309068) talks about the what/why.
- [ ] [This tutorial](https://docs.getdbt.com/docs/introduction) should help get you up and running. Do not hesitate to ask for help configuring your `profiles.yml` file. 
- [ ] Read [how we use dbt](https://gitlab.com/meltano/analytics#dbt), especially our coding conventions.
- [ ] [Official Docs](https://docs.getdbt.com)
- [ ] We use dbt to maintain [our own internal documentation](https://meltano.gitlab.io/analytics/dbt/snowflake/#!/model/model.gitlab_dw.zuora_mrr_totals) on the data transformations we have in place. This is a public link. I suggest bookmarking it. 

Snowflake SQL is probably not that different from the dialects of SQL you're already familiar with, but here are a couple of resources to point you in the right direction:
- [ ] [Differences we found while transition from Postgres to Snowflake](https://gitlab.com/meltano/analytics/issues/645)
- [ ] [How Compatible are Redshift and Snowflake’s SQL Syntaxes?](https://medium.com/@jthandy/how-compatible-are-redshift-and-snowflakes-sql-syntaxes-c2103a43ae84)
- [ ] [Snowflake Functions](https://docs.snowflake.net/manuals/sql-reference/functions-all.html)


## Looker
- [ ] Read our Looker [Readme](https://gitlab.com/gitlab-data/looker/blob/master/README.md).
- [ ] Make an account on training.looker.com. There are four courses. Please take all four, independent of your familiarity with Looker. 
- [ ] [Additional GitLab training resources on Looker](https://gitlab.com/gitlab-data/looker/#training-sessions-gitlab-internal).
- [ ] Watch [Creating Explores Your End Users Love](https://www.youtube.com/watch?v=16N2UMAlzco)


## Misc
- [ ] Familiarize yourself with the Stitch UI, as this is mostly the source of truth for what data we are loading. An email will have been sent with info on how to get logged in.
- [ ] Get setup with Python locally. I suggest using the [Anaconda distribution](https://www.anaconda.com/download/#macos) as it will come pre-packaged with most everything we use.
- [ ] Familiarize yourself with GitLab CI https://docs.gitlab.com/ee/ci/quick_start/ and our running pipelines


## Salesforce 
- [ ] Become familiar with Salesforce using [Trailhead](https://trailhead.salesforce.com/)  
  - [ ] If you are new to Salesforce or CRMs in general, start with [Intro to CRM Basics](https://trailhead.salesforce.com/trails/getting_started_crm_basics).
  - [ ] If you have not used Salesforce before, take this [intro to the platform](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/starting_force_com)
  - [ ] To familiarize yourself with the Salesforce data model, take [Data Modeling](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/data_modeling)
  - [ ] You can review the general data model in [ths reference](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/data_model.htm). Pay particular attention to the [Sales Objects](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_erd_majors.htm)
  - [ ] To familiarize yourself with the Salesforce APIs, take [Intro to SFDC APIs](https://trailhead.salesforce.com/trails/force_com_dev_intermediate/modules/api_basics)

## Zuora
- [ ] Become familiar with Zuora
  - [ ] Watch Brian e plain Zuora to Taylor [GDrive Link](https://drive.google.com/open?id=1fCr48jZbPiW0ViGr-6rZ VVdBpKIoopg)
  - [ ] [Zuora documentation](https://knowledgecenter.zuora.com/)
  - [ ] [Data Model from Zuora for Salesforce](https://knowledgecenter.zuora.com/CA_Commerce/A_Zuora_CPQ/A2_Zuora4Salesforce_Object_Model)
  - [ ] [Data Model inside Zuora](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Zuora_Business_Objects_Relationship)
  - [ ] [Definitions of Objects](https://knowledgecenter.zuora.com/CD_Reporting/D_Data_Sources_and_E ports/AB_Data_Source_Availability)


### Metrics and Methods

- [ ] Read through [SaaS Metrics 2.0](http://www.forentrepreneurs.com/saas-metrics-2/) to get a good understanding of general SaaS metrics.
- [ ] Familiarize yourself with the [GitLab Metrics Sheet] which contains most of the key metrics we use at GitLab and the [definitions of these metrics](https://about.gitlab.com/handbook/finance/operating-metrics/).
