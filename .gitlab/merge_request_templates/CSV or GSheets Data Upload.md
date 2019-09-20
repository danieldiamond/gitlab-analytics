## Request for new data source in Snowflake/Periscope Checklist

<!--
Please complete all items. Ask questions in the #data slack channel
--->

**Original Issue Link**:
<!--
If none, please include a description
--->

**Editor Slack Handle**: @`handle`

**Business Use Case** (Please explain what this data will be used for): 



### DRI/Prioritization Owner Checklist
* [ ]  Provide link to spreadsheet data in Issue. Link: ____
* [ ]  Does this data live in the Data team's data warehouse? (https://gitlab-data.gitlab.io/analytics/dbt/snowflake/#!/overview) [ ] Yes [ ] No 
* [ ]  Does this data need to be linked to other data in the Data team's data warehouse?  [ ] Yes [ ] No 
* [ ]  How long will this data need to reside in the Data team's data warehouse? Expiration Date: ______ 


### Steps to follow for one-off data. 
**This option is used for immediate data use but cannot be linked with other data in the Snowflake warehouse.**
Steps if the user simply needs the data in Periscope but does not need to link it to other data sets in Periscope: 
* [ ]  Submitter please follow steps in [CSV Upload Periscope](https://doc.periscopedata.com/article/csv-upload)
* [ ]  Submitter to close issue if this data will not be used again. 


### Steps to follow if this data needs to reside in our datawarehouse 
**This option is used for the ability to link to other data in the Snowflake warehouse.**
* [ ]  Data Team member to put spreadsheet data into a new file in Sheetload Drive.
* [ ]  Data Team member to share it with the required service account - [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)
* [ ]  Data Team member to check file name and sheet names to match: The file will be located and loaded based on its name. The names of the sheets shared with the runner must be unique and in the `<file_name>.<tab_name>` format
* [ ]  Data Team member to create MR to add this sheet to be pulled in by Sheetload . See: https://gitlab.com/gitlab-data/analytics/merge_requests/1633/diffs as example
* [ ]  Data Team member to ask turn around time needed for data in Periscope. Note: The airflow job for sheetload runs  every night. 
* [ ]  Data Engineer to verify if the data loaded correctly. 
* [ ]  ~~Data Engineer to assign the new table in sheetload schema to correct schemas.~~ Uploads to raw.sheetload
* [ ]  Data Engineer to inform Data Analyst Team that it is now available to have dbt models built. 
  * Taylor Note: We do have a sheetload extract job in CI. So if an MR is made they can clone raw and analytics and do the full test with dbt models in the same MR.
* [ ]  Data Analyst to create dbt models
* [ ]  Data Analyst to assign MR of dbt model to Data Engineer team (iterate until model is complete).
* [ ]  Data Team project maintainers/owners to merge in dbt models 
* [ ]  Data Engineer to run full refresh and inform when available.
  * Or just wait for regular runs to happen. 
* [ ]  Submitter to query in Periscope for table: ``` SELECT * FROM [new-dbt-model-name] LIMIT 10 ```. 

