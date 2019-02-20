## Tools & Access

Goal: To help bring you, our new data analyst, up to speed in the GitLab Data Team's analytics stack as efficiently as possible, without sacrificing quality for speed. 

- [ ] Manager: Add to Analytics API user.
- [ ] Manager: Add to Stitch. 
- [ ] Manager: Add Developer access to Looker.
- [ ] Manager: Add to `@datateam` alias on Slack.
- [ ] Manager: Invite to `data-team` channel on Slack.
- [ ] Manager: Add to Snowflake [following Readme Process](https://gitlab.com/gitlab-data/analytics#managing-roles-for-snowflake). 
- [ ] Manager: Add [future grant](https://docs.snowflake.net/manuals/sql-reference/sql/grant-privilege.html) to analytics schema to user with `grant select on future tables in schema analytics to user [username]`. You will need to be using the `sysadmin` [role](https://docs.snowflake.net/manuals/user-guide/security-access-control-configure.html#assigning-future-grants-on-objects).
- [ ] Manager: Inform new hire what his/her/they scratch schema will be called. **Do not create the schema**, as this will lead to conflicts when dbt runs. 
- [ ] Manager: Invite to SheetLoad folder in gdrive (but not the interviewing data sheet).
- [ ] Manager: Invite to Milestone Planning, Milestone Grooming, and DataOps Meetings. Ping appropriate person to get new hire added to Finance team meetings.
- [ ] Manager: Update issue with one or two Good First Issues. 
- [ ] Manager: Customize this template for the analysts specialty, if any. Delete sections, if appropriate.

WELCOME TO THE TEAM! WE'RE SO EXCITED TO HAVE YOU!!!

- [ ] Read (skim) through this full issue, just so you have a sense of what's coming. 
- [ ] Join the following channels on Slack: `analytics`, `analytics-pipelines`, and `business-operations`.
- [ ] Schedule a recurring monthly skip level meeting with the Director of Business Operations.
- [ ] Read the following pages of the handbook in their entirety. 
   - [ ] [Data Team](https://about.gitlab.com/handbook/business-ops/data-team/index.html)
   - [ ] [Business Operations](https://about.gitlab.com/handbook/business-ops/)
   - [ ] [Data Quality Process](https://about.gitlab.com/handbook/business-ops/data-quality-process/)
- [ ] Watch @tlapiana's [talk at DataEngConf](https://www.youtube.com/watch?v=eu623QBwakc) that gives a phenomenal overview of how the team works.
- [ ] Create a new issue in the Analytics project (this project). As you proceed and things are unclear, document it in the issue. Don't worry about organizing it; just brain dump it into the issue! This will help us iterate on the onboarding process.
- [ ] We suggest installing [iTerm2](http://iterm2.com) for use, over the Terminal. No pressure though.
- [ ] Get setup with Python locally. We suggest using the [Anaconda distribution](https://www.anaconda.com/download/#macos) as it will come pre-packaged with most everything we use.


Our data stack looks roughly like this:
<img src = "https://cdn-images-1.medium.com/max/2000/1*BogoeTTK1OXFU1hPfUyCFw.png">

As you read in the handbook, we currently use Stitch and Meltano for extracting data from its raw sources and loading it into our Snowflake data warehouse. We use open source dbt (more on this in a moment) as our transformation tool and Looker as our business intelligence/data visualization tool. The bulk of your projects and tasks will be in dbt and Looker, so we will spend a lot of time familiarizing yourself with those tools and then dig into specific data sources. 

## Connecting to Snowflake
- [ ] Download a SQL development tool that is compatible with Snowflake, such as [SQLWorkbench/J](http://sql-workbench.net) or [DataGrip](https://www.jetbrains.com/datagrip/). If you're interested in DataGrip, follow the [instructions to get a JetBrains license in the handbook](https://about.gitlab.com/handbook/tools-and-tips/#jetbrains). Alternatively, Snowflake has a Web UI for querying the data warehouse that can be found under [Worksheets](https://gitlab.snowflakecomputing.com/console#/internal/worksheet).
   - [ ] If using the Snowflake Web UI, update your role to `TRANSFORMER`, warehouse to `TRANSFORMING`, and database to `ANALYTICS`. The schema does not matter because your query will reference the schema. 
   - [ ] If using DataGrip, you may need to download the [Driver](https://docs.snowflake.net/manuals/user-guide/jdbc-download.html#downloading-the-driver).
   - [ ] If using DataGrip with Snowflake, you need to make a change to your configuration. Go to `Help > Edit Custom VM Options ...`. Then add the line `-Duser.timezone=UTC`.
   - [ ] This template may be useful as you're configuring the DataGrip connection to Snowflake `jdbc:snowflake://{account:param}.snowflakecomputing.com/?{password}[&db={Database:param}][&schema={Schema:param}][&warehouse={Warehouse:param}][&role={Role:param}]`

## dbt

### What is dbt?
- [ ] Familiarize yourself with [dbt](https://www.getdbt.com/), which we use for transformations in the data warehouse, as it gives us a way to source control the SQL. 
- [ ] [This article](https://blog.fishtownanalytics.com/what-exactly-is-dbt-47ba57309068) talks about the what/why.
- [ ] [This introduction](https://docs.getdbt.com/docs/introduction) should help get you understand what dbt is.
- [ ] Read [how we use dbt](https://gitlab.com/gitlab-data/analytics#dbt), especially our coding conventions.
- [ ] Watch [video](https://drive.google.com/file/d/1ZuieqqejDd2HkvhEZeOPd6f2Vd5JWyUn/view) of Taylor introducing Chase to dbt.
- [ ] Peruse the [Official Docs](https://docs.getdbt.com).
- [ ] In addition to using dbt to manage our transformations, we use dbt to maintain [our own internal documentation](https://gitlab-data.gitlab.io/analytics/dbt/snowflake/#!/overview) on those data transformations. This is a public link. We suggest bookmarking it. 
- [ ] Read about and and watch [Drew demo dbt docs to Emilie & Taylor](https://blog.fishtownanalytics.com/using-dbt-docs-fae6137da3c3). Read about [Scaling Knowledge](https://blog.fishtownanalytics.com/scaling-knowledge-160f9f5a9b6c) and the problem we're trying to solve with our documentation.
- [ ] Consider joining [dbt slack](https://slack.getdbt.com) (Not required, but strongly recommended).
- [ ] Information and troubleshooting on dbt is sparse on Google & Stack Overflow, we recommend the following sources of help when you need it: 
   * Your teammates! We are all here to help!
   * dbt slack has a #beginners channel and they are very helpful.
   * [Fishtown Analytics Blog](https://blog.fishtownanalytics.com)
   * [dbt Discourse](http://discourse.getdbt.com)


### Getting Set up with dbt locally
- [ ] Install dbt via [Homebrew](https://brew.sh) with `brew tap fishtown-analytics/dbt` and `brew install dbt`.
- [ ] Run `dbt --version` to know you've successfully installed dbt. 
- [ ] In your root directory, please run `mkdir .dbt`. Then create a `profiles.yml` file that looks as follow:
```  
gitlab-snowflake: 
  target: dev
  outputs:
    dev:
      type: snowflake
      threads: 8
      account: gitlab
      user: %%YOUR USER%%
      password: %%YOUR PASSWORD%%
      role: TRANSFORMER
      database: ANALYTICS
      warehouse: TRANSFORMING
      schema: %%YOUR SCRATCH%%
```
- All dbt commands need to be run within the analytics project, specifically you must be in `analytics/transform/snowflake-dbt` or below. 
- [ ] Run `dbt compile` to know that your connection has been successful, you are in the correct location, and everything will run smoothly. 
- [ ] When you are ready to start working with dbt start by running `dbt deps` and doing a `dbt run`. This will take some time (estimate 1 hour, though it could be longer) the first time it runs. 

Here is your dbt commands cheat sheet:
 * `dbt compile` - compiles all models
 * `dbt run` - regular run
 * `dbt run --models modelname` - will only run modelname
 * `dbt run --models +modelname` - will run modelname and all the models it depends on
 * `dbt run --models modelname+` - will run modelname and all the models that depend on it
 * `dbt run --models +modelname+` - will run modelname, all the models it depends on, and all the models that depend on it
 * `dbt run --exclude modelname` - will run all models except modelname
 * `dbt run --full-refresh` - will refresh incremental models
 * `dbt test` - will run custom data tests and schema tests; TIP: `dbt test` takes the same `--model` and `--exclude` syntax referenced for `dbt run`
 * `dbt docs generate` - will generate files for docs
 * `dbt docs serve` - will serve the dbt docs locally

## Snowflake SQL
Snowflake SQL is probably not that different from the dialects of SQL you're already familiar with, but here are a couple of resources to point you in the right direction:
- [ ] [Differences we found while transition from Postgres to Snowflake](https://gitlab.com/gitlab-data/analytics/issues/645)
- [ ] [How Compatible are Redshift and Snowflake’s SQL Syntaxes?](https://medium.com/@jthandy/how-compatible-are-redshift-and-snowflakes-sql-syntaxes-c2103a43ae84)
- [ ] [Snowflake Functions](https://docs.snowflake.net/manuals/sql-reference/functions-all.html)

## Looker
- [ ] Read our Looker [Readme](https://gitlab.com/gitlab-data/looker/blob/master/README.md).
- [ ] Make an account on [training.looker.com](training.looker.com). There are four courses. Please take all four, independent of your familiarity with Looker. 
- [ ] [Additional GitLab training resources on Looker](https://gitlab.com/gitlab-data/looker/#training-sessions-gitlab-internal).
- [ ] Watch [Creating Explores Your End Users Love](https://www.youtube.com/watch?v=16N2UMAlzco)
- [ ] Add a [custom user attribute](https://gitlab.looker.com/admin/user_attributes) for "sandbox_schema" to refer to your sandbox dbt schema.
- [ ] [Looker Codev Training 1](https://drive.google.com/file/d/1sKHbARpIfHKGpTChuqZSagnfh8Vt7_ml/view?usp=sharing)
- [ ] [Looker Codev Training 2](https://drive.google.com/file/d/1wNM-xnkDOBXce-M0cX16pkiFjsf3woma/view?usp=sharing)
- [ ] [Looker Codev Training 3](https://drive.google.com/file/d/1bKBtrCGxVRwXpYuYMXoD4XAqM1lzgdqL/view?usp=sharing)
- [ ] [Looker Codev Training 4](https://drive.google.com/file/d/1xZbXVG85tA388r57QpRPR4-eLi54ixhL/view?usp=sharing)
- [ ] [Looker Codev Training 5](https://drive.google.com/file/d/1RS3ALTjxh8VaNwt-q94Wbv_OYibsPzeR/view?usp=sharing)
- [ ] [Looker Business User Training](https://drive.google.com/file/d/19RzwdtRDNWvDL7W81_CfjX6sWDo_nP2w/view?usp=sharing)

## Misc
- [ ] Familiarize yourself with the [Stitch](http://stitchdata.com) UI, as this is mostly the source of truth for what data we are loading. An email will have been sent with info on how to get logged in.
- [ ] Familiarize yourself with GitLab CI https://docs.gitlab.com/ee/ci/quick_start/ and our running pipelines.
- [ ] Consider joining [Locally Optimistic slack](https://www.locallyoptimistic.com/community/)
 (Not required, but recommended).
- [ ] Consider subscribing to the [Data Science Roundup](http://roundup.fishtownanalytics.com) (Not required, but recommended).
- [ ] There are many Slack channels organized around interests, such as `#fitlab`, `#bookclub`, and `#woodworking`. There are also many organized by location (these all start with `#loc_`). This is a great way to connect to GitLabbers outside of the team. Join some that are relevant to your interests, if you'd like. 
- [ ] Familiarize yourself with [SheetLoad](https://about.gitlab.com/handbook/business-ops/data-team/#using-sheetload).

## GitLab.com aka "Dot Com" (Product)
This data comes from our GitLab.com SaaS product.
- [ ] Become familiar with the [API docs](https://gitlab.com/gitlab-org/gitlab-ee/tree/master/doc/api).

## Marketo
- [ ] [Coming soon]
- [ ] For access to Marketo, you will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request).

## Netsuite (Accounting)
- [ ] [Coming soon]
- [ ] For access to Netsuite, you will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request).

## Pings (Product)
This data comes from the usage ping that comes with a GitLab installation.
- [ ] Read about the [usage ping](https://docs.gitlab.com/ee/user/admin_area/settings/usage_statistics.html).
- [ ] To understand how this is implemented at GitLab read [Feature Implementation](https://about.gitlab.com/handbook/product/feature-instrumentation/#instrumentation-for-gitlabcom).
- [ ] There is not great documentation on the usage ping, but you can get a sense from looking at the `usage.rb` file for [GitLab CE](https://gitlab.com/gitlab-org/gitlab-ee/blob/master/lib/gitlab/usage_data.rb).
- [ ] It might be helpful to look at [issues related to the usage pings](https://gitlab.com/groups/gitlab-org/-/issues?scope=all&utf8=✓&state=all&label_name%5B%5D=usage%20ping).

## Salesforce (Sales, Marketing, Finance)
- [ ] Become familiar with Salesforce using [Trailhead](https://trailhead.salesforce.com/).  
- [ ] If you are new to Salesforce or CRMs in general, start with [Intro to CRM Basics](https://trailhead.salesforce.com/trails/getting_started_crm_basics).
- [ ] If you have not used Salesforce before, take this [intro to the platform](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/starting_force_com).
- [ ] To familiarize yourself with the Salesforce data model, take [Data Modeling](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/data_modeling).
- [ ] You can review the general data model in [this reference](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/data_model.htm). Pay particular attention to the [Sales Objects](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_erd_majors.htm).
- [ ] To familiarize yourself with the Salesforce APIs, take [Intro to SFDC APIs](https://trailhead.salesforce.com/trails/force_com_dev_intermediate/modules/api_basics).
- [ ] For access to SFDC, you will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request).

## Snowplow (Product)
[Snowplow](https://snowplowanalytics.com) is an open source web analytics collector. 
- [ ] To understand how this is implemented at GitLab read [Feature Implementation](https://about.gitlab.com/handbook/product/feature-instrumentation/#instrumentation-for-gitlabcom).
- [ ] Familiarize yourself with the [Snowplow Open Source documentation](https://github.com/snowplow/snowplow).
- [ ] We use the [Snowplow dbt package](https://hub.getdbt.com/fishtown-analytics/snowplow/latest/) on our models. Their documentation does show up in our dbt docs.

## Zendesk
- [ ] For access to Zendesk, you will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request).

## Zuora (Finance, Billing SSOT)
- [ ] Become familiar with Zuora.
- [ ] Watch Brian explain Zuora to Taylor [GDrive Link](https://drive.google.com/file/d/1fCr48jZbPiW0ViGr-6rZxVVdBpKIoopg/view).
- [ ] [Zuora documentation](https://knowledgecenter.zuora.com/).
- [ ] [Data Model from Zuora for Salesforce](https://knowledgecenter.zuora.com/CA_Commerce/A_Zuora_CPQ/A2_Zuora4Salesforce_Object_Model).
- [ ] [Data Model inside Zuora](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Zuora_Business_Objects_Relationship).
- [ ] [Definitions of Objects](https://knowledgecenter.zuora.com/CD_Reporting/D_Data_Sources_and_Exports/AB_Data_Source_Availability).
- [ ] [Zuora Subscription Data Management](https://about.gitlab.com/handbook/finance/zuora-sub-data/).
- [ ] For access to Zuora, you will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request).

### Metrics and Methods
- [ ] Read through [SaaS Metrics 2.0](http://www.forentrepreneurs.com/saas-metrics-2/) to get a good understanding of general SaaS metrics.
- [ ] Familiarize yourself with the GitLab Metrics Sheet (search in Google Drive, it should come up) which contains most of the key metrics we use at GitLab and the [definitions of these metrics](https://about.gitlab.com/handbook/finance/operating-metrics/).

## Suggested Bookmarks
None of these are required, but bookmarking these links will make life at GitLab much easier. Some of these are not hyperlinked for security concerns. 
- [ ] [Company Call Agenda](https://docs.google.com/document/d/1JiLWsTOm0yprPVIW9W-hM4iUsRxkBt_1bpm3VXV4Muc/edit)
- [ ] [DataOps Meeting Agenda](https://docs.google.com/document/d/1qCfpRRKQfSU3VplI45huE266CT0nB82levb3lF9xeUs/edit)
- [ ] 1:1 with Manager Agenda
- [ ] [Create new issue in Analytics Project](https://gitlab.com/gitlab-data/analytics/issues/new?issue%5Bassignee_id%5D=&issue%5Bmilestone_id%5D=)
- [ ] [Active Milestone](https://gitlab.com/gitlab-data/analytics/issues?scope=all&utf8=✓&state=opened&milestone_title=%23started) (You may want to update this link to filter to just your issues.)
- [ ] [Data team page of Handbook](https://about.gitlab.com/handbook/business-ops/data-team/)
- [ ] [dbt Docs](https://docs.getdbt.com)
- [ ] [dbt Discourse](http://discourse.getdbt.com)
- [ ] [GitLab's dbt Documentation](https://gitlab-data.gitlab.io/analytics/dbt/snowflake/#!/overview)
- [ ] [Looker Discourse](https://discourse.looker.com)

## Good First Issues:
- [ ] [Replace]
- [ ] [Replace]
