[![pipeline status](https://gitlab.com/gitlab-data/analytics/badges/master/pipeline.svg)](https://gitlab.com/gitlab-data/analytics/commits/master)

## Quick Links
* Data Team Handbook - https://about.gitlab.com/handbook/business-ops/data-team/#-quick-links
* dbt Docs - https://gitlab-data.gitlab.io/analytics/dbt/snowflake/#!/overview
* Epics Roadmap - https://gitlab.com/groups/gitlab-data/-/roadmap?layout=MONTHS&sort=start_date_asc
* Snowflake Web UI - https://gitlab.snowflakecomputing.com
* [Machine Learning Resources](https://drive.google.com/drive/folders/1sOXWW-FujwKU2T-auG7KPR9h6xqDRx0z?usp=sharing) (GitLab Internal)
* [Email Address to Share Sheetloaded Doc with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)

## GitLab Data Team

This is the primary repository of the Data Team at GitLab. The Data Team is a part of the Finance organization within GitLab, but we serve the entire company. We do this by maintaining a data warehouse where information from all business operations is stored and managed for analysis.

Our charter and goals are as follows:

* Build a centralized data warehouse that can support data analysis requirements from all functional groups within the company.
* Create a common data framework and governance practice.
* Establish the single source of truth for company metrics.
* Establish a change management processes for source systems.
* Develop a Data Architecture plan (in conjunction with functional teams).
* Develop a roadmap for systems evolution in alignment with the Company’s data architecture plan.

## Priorities

Like the rest of the company, we set quarterly objectives and key results. These are available on our company OKR page.

## Media
* [How Data Teams Do More With Less By Adopting Software Engineering Best Practices - Thomas's talk at the 2018 DataEngConf in NYC](https://www.youtube.com/watch?v=eu623QBwakc)

* [Taylor explains dbt](https://drive.google.com/open?id=1ZuieqqejDd2HkvhEZeOPd6f2Vd5JWyUn) (GitLab internal)

* [dbt docs intro with Drew Banin from Fishtown Analytics](https://www.youtube.com/watch?v=bqIBNvA9xjo)

* [Tom Cooney explains Zendesk](https://drive.google.com/open?id=1oExE1ZM5IkXcq1hJIPouxlXSiafhRRua) (GitLab internal)

* [Luca Williams explains Customer Success Dashboards](https://drive.google.com/open?id=1FsgvELNmQ0ADEC1hFEKhWNA1OnH-INOJ) (GitLab internal)

* [Art Nasser explains Netsuite and Campaign Data](https://drive.google.com/open?id=1KUMa8zICI9_jQDqdyN7mGSWSLdw97h5-) (GitLab internal)

* [Courtland Smith explains Marketing Dashboard needs](https://drive.google.com/open?id=1bjKWRdfUgcn0GfyB2rS3qdr_8nbRYAZu) (GitLab internal)

* Taylor's talk at the 2018 Nashville Analytics Summit - Coming Soon

* GitLab blog post about Meltano - https://news.ycombinator.com/item?id=17667399
  * Livestream chat with Sid and HN user slap_shot - https://www.youtube.com/watch?v=F8tEDq3K_pE
  * Follow-up blog post to original Meltano post - https://about.gitlab.com/2018/08/07/meltano-follow-up/

* Data Source Overviews:
   * [Pings](https://drive.google.com/file/d/1S8lNyMdC3oXfCdWhY69Lx-tUVdL9SPFe/view)

## Recommended Reading, Listening, Watching

* [The AI Hierarchy of Needs](https://hackernoon.com/the-ai-hierarchy-of-needs-18f111fcc007)
* [Data Meta Metrics](https://caitlinhudon.com/2017/11/14/data-meta-metrics/)
* [Engineers Shouldn’t Write ETL](https://multithreaded.stitchfix.com/blog/2016/03/16/engineers-shouldnt-write-etl/)
* [The Startup Founder’s Guide to Analytics](https://thinkgrowth.org/the-startup-founders-guide-to-analytics-1d2176f20ac1)
* [Functional Data Engineering — a modern paradigm for batch data processing](https://medium.com/@maximebeauchemin/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a)
* [Keep it SQL Stupid, a talk by Connor McArthur of Fishtown Analytics at DataEngConf SF '18 explaining dbt](https://www.youtube.com/watch?v=9VNh11qSfAo)
* [Views on Vue Podcast with Jacob Schatz and Taylor Murphy](https://devchat.tv/views-on-vue/vov-030-how-we-use-vue-in-data-science-with-jacob-schatz-taylor-murphy-gitlab-team/)
* [Pain Points by the Airflow Podcast with @tlapiana](https://soundcloud.com/the-airflow-podcast/pain-points)
* [DevOps for AI](https://www.youtube.com/watch?v=HwZlGQuCTj4)
* [What Can Data Scientists Learn from DevOps](https://redmonk.com/dberkholz/2012/11/06/what-can-data-scientists-learn-from-devops/)
* [One Analyst's Guide for Going from Good to Great](https://blog.fishtownanalytics.com/one-analysts-guide-for-going-from-good-to-great-6697e67e37d9)
* The Value of Data: [Part 1](https://www.codingvc.com/the-value-of-data-part-1-using-data-as-a-competitive-advantage), [Part 2](https://www.codingvc.com/the-value-of-data-part-2-building-valuable-datasets), [Part 3](https://www.codingvc.com/the-value-of-data-part-3-data-business-models)
* [Building a Data Practice](https://www.locallyoptimistic.com/post/building-a-data-practice/)
* [Does my Startup Data Team Need a Data Engineer](https://blog.fishtownanalytics.com/does-my-startup-data-team-need-a-data-engineer-b6f4d68d7da9)
* [Data Science is different now](https://veekaybee.github.io/2019/02/13/data-science-is-different/) (Note: this is why GitLab doesn't have a Data Scientist yet.)
* [Resources Written by dbt Community Members](https://discourse.getdbt.com/t/resources-written-by-community-members/276)

## Documentation

### Airflow

We use Airflow for Orchesetration, and this includes testing in MRs. The following is the new MR workflow that includes testing with Airflow.

#### Airflow in Production

All DAGs are created using the `KubernetesPodOperator`, so the airflow pod itself has minimal dependencies and doesn't need to be restarted unless a major infrastructure change takes place.
There are 4 containers running in the current Airflow deployment:

1. A sidecar container checks the repo activity feed for any merges to master. If there was one, the sidecar will reclone the repo so that Airflow runs the freshest DAGs.
2. The Airflow scheduler
3. The Airflow webserver
4. A cloudsql proxy that allows Airflow to connect to our cloudsql instance

#### Airflow in MRs

To facilitate the easier use of Airflow locally while still testing properly running our DAGs in Kubernetes, we use docker-compose to spin up local Airflow instances that then have the ability to run their DAG in Kubernetes using the KubernetesPodOperator.
The flow from code change to testing in Airflow should look like this (this assumes there is already a DAG for that task):

1. Commit and push your code to the remote branch.
2. Run `make set-branch`. This will spit out an environment variable you need to set for your branch. 
2. Run `make init-airflow` to spin up the postgres db container and init the Airflow tables. You will get an error if Docker is not running.
3. Run `make airflow` to spin up Airflow and attach a shell to one of the containers
4. Open a web browser and navigate to `localhost:8080` to see your own local webserver
5. In the airflow shell, run a command to trigger the DAG/Task you want to test, for example `airflow run snowflake_load snowflake-load 2019-01-01` (as configured in the docker-compose file, all kube pods will be created in the `testing` namespace). Or if you want to run an entire DAG (for instance the `dbt` DAG to test the branching logic), the command would be something like `airflow backfill dbt -s 2019-01-01T00:00:00 -e 2019-01-01T00:00:00`.
6. Once the job is finished, you can navigate to the DAG/Task instance to review the logs.

There is also a `make help` command that describes what commands exist and what they do.

#### Variable Requirements for using Airflow in the MR workflow

```
KUBECONFIG
GOOGLE_APPLICATION_CREDENTIALS
GIT_BRANCH
```

As long as those are set, the docker-compose file will correctly configure all permissions.

#### Video Walk Throughs

* [Airflow pt 1](https://drive.google.com/open?id=1S03mMINXJFXekeixcJS2tN4T62qYchej)
* [Airflow pt 2](https://drive.google.com/open?id=1zZGtSZIvSwHvhu2sEgGm4LjvbLim5KME)

##### Project variables

Our current implementation uses the following project variables:

  - SNOWFLAKE_ACCOUNT
  - SNOWFLAKE_REPORT_WAREHOUSE
  - SNOWFLAKE_{FLAVOR}_USER
  - SNOWFLAKE_{FLAVOR}_PASSWORD
  - SNOWFLAKE_{FLAVOR}_DATABASE
  - SNOWFLAKE_{FLAVOR}_ROLE
  - SNOWFLAKE_{FLAVOR}_WAREHOUSE
  
The following flavors are defined:

  - `LOAD` flavor is used by the Extract & Load process
  - `TRANSFORM` flavor is used by the Transform process
  - `TEST` flavor for testing using Snowflake
  - `PERMISSION` flavor for the permission bot
  - `SYSADMIN` flavor for housekeeping tasks (like setting up review instances). This flavor doesn't define `SNOWFLAKE_SYSADMIN_DATABASE` and `SNOWFLAKE_SYSADMIN_WAREHOUSE`.

<div class="panel panel-warning">
The following variables are set at the job level dependending on the running environment **and should not set in the project settings**.
{: .panel-heading}
<div class="panel-body">
  - SNOWFLAKE_USER
  - SNOWFLAKE_PASSWORD
  - SNOWFLAKE_ROLE
  - SNOWFLAKE_DATABASE
  - SNOWFLAKE_WAREHOUSE
</div>
</div>

### Accessing peered VPCs

Some of the GitLab specific ELTs connect to databases which are in peered GCP projects, such as the usage ping. To allow connections, a few actions have been taken:
1. The Kubernetes cluster where the runner executes has been setup to use [IP aliasing](https://cloud.google.com/kubernetes-engine/docs/how-to/ip-aliases), so each pod gets a real routable IP within GCP.
1. A [VPC peering relationship](https://cloud.google.com/vpc/docs/vpc-peering) has been established between the two projects and their networks.
1. A firewall rule has been created in the upstream project to allow access from the runner Kubernetes cluster's pod subnet.

#### Hosts Records Dataflow

From our on-premises installations, we recieve [version and ping information](https://docs.gitlab.com/ee/user/admin_area/settings/usage_statistics.html) from the software. This data is currently imported once a day from a PostgreSQL database into our enterprise data warehouse (EDW). We use this data to feed into Salesforce (SFDC) to aid our sales representatives in their work.

The domains from all of the pings are first cleaned by standardizing the URL using a package called [tldextract](https://github.com/john-kurkowski/tldextract). Each cleaned ping type is combined into a single host record. We make a best effort attempt to align the pings from the same install of the software. 

This single host record is then enriched with data from three sources: DiscoverOrg, Clearbit, and WHOIS. If DiscoverOrg has no record of the domain we then fallback to Clearbit, with WHOIS being a last resort. Each request to DiscoverOrg and Clearbit is cached in the database and is updated no more than every 30 days. The cleaning and enrichment steps are all accomplished using Python.

We then take all of the cleaned records and use dbt to make multiple transformations. The last 60 days of pings are aligned with Salesforce accounts using the account name or the account website. Based on this, tables are generated of host records to upload to SFDC. If no accounts are found, we then generate a table of accounts to create within SFDC. 

Finally, we use Python to generate SFDC accounts and to upload the host records to the appropriate SFDC account. We also generate any accounts necessary and update any SFDC accounts with DiscoverOrg, Clearbit, and WHOIS data if any of the relevant fields are not already present in SFDC.

##### Data Grip Configuration

You can change your formatting preferences in Data Grip by going to Preferences > Editor > Code Style > HTML.
You should have:

* Use tab character: unchecked
* Tab size: 4
* Indent: 4
* Continuation indent: 8
* Keep indents on empty lines: unchecked

You can use `Command + Option + L` to format your file.

#### Data Resources
- [Fishtown Analytics Blog](https://blog.fishtownanalytics.com)
- [Silota](http://www.silota.com/docs/recipes/)
- [Mode Analytics Blog](https://blog.modeanalytics.com/)
- [Periscope Data Blog](https://www.periscopedata.com/blog)
- [Yhat Blog](http://blog.yhat.com/)
- [Wes McKinney Blog](http://wesmckinney.com/archives.html)
- [MBA Mondays](https://avc.com/archive/#mba_mondays_archive)

#### Data Newsletters
- [Data Science Roundup Newsletter](http://roundup.fishtownanalytics.com/)
- [SF Data](http://weekly.sfdata.io/)
- [Data is Plural](https://tinyletter.com/data-is-plural)
- [DataEng Weekly](http://dataengweekly.com/)
- [Data Elixir](https://dataelixir.com/)
- [Data Science Weekly](https://www.datascienceweekly.org/)

#### Data Visualization Resources
- [Storytelling with Data](http://storytellingwithdata.com/)
- [Eager Eyes](https://eagereyes.org/)
- [FiveThirtyEight's DataLab](https://fivethirtyeight.com/features/)
- [Flowing Data](https://flowingdata.com/)
- [The Functional Art](http://www.thefunctionalart.com/)
- [PolicyViz](https://policyviz.com/)
- [JunkCharts](https://junkcharts.typepad.com/)
- [Make a Powerful Point](http://makeapowerfulpoint.com/)
- [Perceptual Edge](https://perceptualedge.com/)
- [VizWiz](http://www.vizwiz.com/)
- [Visualising Data](http://www.visualisingdata.com/)
- [WTF Visualizations](http://viz.wtf/)
- [The Pudding](https://pudding.cool/)


## Contributing to the Data Team project

We welcome contributions and improvements, please see the [contribution guidelines](CONTRIBUTING.md).


## License

This code is distributed under the MIT license, see the [LICENSE](LICENSE) file.
