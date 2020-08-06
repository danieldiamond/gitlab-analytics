## Data Triage 

<!--
Please complete all items. Ask questions in the #data slack channel
--->

## Housekeeping 
* [ ] Title the issue as "<ISO date> Data Triage" e.g. 2020-07-09 Data Triage
* [ ] Assign this issue to both the Data Analyst and Data Engineer assigned to Triage 
* [ ] [Add a weight to the issue](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-pointing)
* [ ] Link any issues opened as a result of Data Triage to this `parent` issue. 
* [ ] Close this issue once all the items below are completed. 

## Data Analyst tasks
All tasks below should be checked off on your Triage day. 
This is the most important issue on your Triage day. 
Please prioritize this issue since we dedicate a day from your milestone to this activity. 

### Reply to slack channels 
* [ ] Review each slack message request in the **#data** and **#data-lounge** channel 
    - [ ] Reply to slack threads by pointing GitLabbers to the appropriate handbook page, visualization, or to other GitLabbers who may know more about the topic. 
    - [ ] Direct GitLabbers to the channel description, which has the link to the Data team project, if the request requires more than 5 minutes of investigative effort from a Data team member.
* [ ] Review each slack message in the **#data-triage** channel, which will inform the triager of what issues have been opened in the data team project that day. 
    - [ ] For each issue opened by a non-Data Team member, label the issue by: 
        - [ ] Adding the `Workflow::start (triage)` and `Triage` label
        - [ ] Adding additional [labels](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-labeling)
        - [ ] Assigning the issue based on:
            - [ ] the [CODEOWNERS file](https://gitlab.com/gitlab-data/analytics/blob/master/CODEOWNERS) for specific dbt model failures 
            - [ ] the [functional DRIs](https://about.gitlab.com/handbook/business-ops/data-team/organization/#team-organization)
            - [ ] OR to the  Manager, Data if you aren't sure. 
        - [ ] Asking initial questions (data source, business logic clarification, etc) to groom the issue. 

### Maintain KPI related information         
* [ ] Maintain the KPI Index page by 
    - [ ] Creating an issue with any outstanding concerns from your respective division (broken links, missing KPI definitions, charts vs links, etc)
    - [ ] Assigning the issue to the [functional DRIs](https://about.gitlab.com/handbook/business-ops/data-team/organization/#team-organization)
* [ ] Review the commit history of the following two files and update the [sheetload_kpi_status data](https://docs.google.com/spreadsheets/d/1CZLnXiAG7D_T_6vm50X0hDPnMPKrKmtajrcga5vyDTQ/edit?usp=sharing) with any new KPIs or update existing KPI statistics (`commit_start` column is the commit URL for the `start_date` and `commit_handbook_v1` column is the commit URL for the `completion_date`)
    - [ ] [KPI Index handbook page](https://gitlab.com/gitlab-com/www-gitlab-com/-/commits/master/source/handbook/business-ops/data-team/kpi-index/index.html.md.erb)
    - [ ] [Engineering KPI list](https://gitlab.com/gitlab-com/www-gitlab-com/-/blob/master/data/performance_indicators.yml)

### Prepare for Next Milestone 
* [ ] Groom Issues for Next Milestone: for issues that have missing or poor requirements, add comments in the issue asking questions to the Business DRI. 
* [ ] Update the planning issues for this milestone and the next milestone 
* [ ] Close/clean-up any irrelevant issues in your backlog. 


## Data Engineer tasks

* [ ] [Create issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=DE%20Triage%20Errors) for each new failure in the **#dbt-runs** 
* [ ] [Create issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=DE%20Triage%20Errors) for each new failure in **#analytics-pipelines**
    * [ ] Link to all resulting issues and MRs in slack 
* [ ] Check the DBT Source Freshness Dashboard for lagging or missing data. 
    * [ ] Link to all resulting issues and MRs in slack 

In addition to these tasks the Data Engineer on triage should be focused on resolving these issues, including the backlog found on the [DE - Triage Errors board](https://gitlab.com/groups/gitlab-data/-/boards/1917859)

## Finishing the Day

* [ ] At the end of your working day post EOD message to slack along with a link to this issue in the above mentioned slack channels so that it is clear for the next triager what time to check for issues from.





/label ~"workflow::In dev" ~"Housekeeping" ~"Data Team" ~"Documentation" ~"Triage" ~"Priority::1-Ops"
