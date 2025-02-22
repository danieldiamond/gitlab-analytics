## Request for new data source in Snowflake/Periscope Checklist

<!--
Please complete all items. Ask questions in the #data slack channel
--->

**Original Issue Link**:
<!--
If none, please include a description
--->

**Submitter Slack Handle**: @`handle`

**Business Use Case (Please explain what this data will be used for):** 


## DRI/Prioritization Owner Checklist
* [ ] This data does not exist live in the Data team's data warehouse? (https://dbt.gitlabdata.com) 
* [ ] Do any objects in this data source need to be snapshotted? If yes, please open separate issues to have the objects snapshotted.
* [ ] Does this data contain anything that is sensitive (Classified as Red or Orange in our [Data Classification Policy](https://about.gitlab.com/handbook/engineering/security/data-classification-policy.html#data-classification-levels))?
  - [ ] Yes 
  - [ ] No
  - [ ] I don't know
* [ ] Does this data have any agreed [SLO](https://about.gitlab.com/handbook/business-ops/data-team/platform/#slos-service-level-objectives-by-data-source) attached to it? If not: 
    * [ ] How often does the data need to be refreshed? 

## Integration Preparation 

<!--
Sufficient access needs to be granted and verified before we can begin working on an automated extraction
--->

**Will there need to be access granted in order for a Data Engineer to extract this data? (example: New permissions or credentials to Salesforce in order to access the data)**
  - [ ] Yes 
  - [ ] No
  - [ ] I don't know

**If Yes:**
- Prioritise giving access to a service account rather than any individual Data Engineer, if uncertain on which account to 
use contact the Data Engineer assigned below for confirmation.  
- Where will access be required? 
- Link to Access Request: <!-- This can be blank to start, will need to be added for prioritization -->


## Data Engineer tasks**
**API** 
  * [ ] If using one of our third party API handlers (Stitch/FiveTran) the integration should be confirmed to be up and running
  * [ ] If writing a custom API integration the API endpoints should be confirmed to be active and returning expected data. 
 

**Admin**
  * [ ] Create issue for creation of extract process (not needed if using Stitch/FiveTran)
  * [ ] Create issue for dbt models 
  * [ ] Create and link merge requests for updating relevant documentation 

## Data Use / Acceptance Criteria 

**Who should be responsible for making the data accessible and usable in the data warehouse?**
- Analyst: <!-- please tag them -->
- Data Engineer: <!-- please tag them -->

**Who will be using this data, and where (dashboards, snowflake UI, etc.)?**
