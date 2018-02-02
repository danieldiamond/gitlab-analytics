# Development Plan

For the MVP of BizOps, we plan to delivering the following based on [first objective](../#objectives) of the project:

* A configurable ELT engine to retrieve data out of SFDC, Zuora, and Marketo
* A BI dashboard to view the ELT'd data
* Sample dashboards to get started

This would provide a basic foundation for analyzing your CRM data that is contained within SFDC.

## Sprints

**Now**

1. Finish foundational work - get Metabase up with proper security
1. [Enable internal teams to use Metabase on their own / create new SSOT dashboards](https://docs.google.com/presentation/d/e/2PACX-1vTPuXqackF1kHW-GqsDmZAxuof0IbQNQrzg9IyKPYs5Utkzae4bOeOCoLNbJ6gZ2Rj4YCDjzImTmcDV/pub?start=false&loop=false&delayms=3000&slide=id.g329fdfcfcc_0_58)
1. SAO by source and by campaign
1. CAC by channel

**Next**

* Automate and create visualizations of the [GitLab metrics sheet](https://docs.google.com/spreadsheets/d/1-HjIWMwJZ9nUxc9XKXIIps3pgR_9VyocpG7YN0dCVZ4/edit#gid=692213658). Need to further differentiate priority within this set.
* All of InsightSquared
* All metrics that are in OKRs visualized

**Backlog**

* Automate & provide guide rails for ELT phase
  * Resilient and automated process to populate staging tables from data sources (for example, Pentaho's fragility/complexity)
  * Create script to check user provided mapping file for required fields (staging field -> data model field), list missing ones
* Distinguish between custom GitLab dashboards and public dashboards?
* Identify an easy "flow" to save modified dashboard into repo. (Cut/Paste, download file, etc.)
