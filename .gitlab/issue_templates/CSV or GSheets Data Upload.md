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
* [ ]  Provide link to CSV/GSheet data. Link: ____
* [ ]  Does this data live in the Data team's data warehouse? (https://gitlab-data.gitlab.io/analytics/dbt/snowflake/#!/overview) 
  - [ ] Yes 
  - [ ] No 
* [ ]  Does this data need to be linked to other data in the Data team's data warehouse?
  - [ ] Yes 
  - [ ] No
* [ ]  How long will this data need to reside in the Data team's data warehouse? Expiration Date: ______ 
* [ ]  How do you want to name the table? Table Name: ______ 
* [ ]  Update the due date on this issue for when you want this data in Periscope. (Note: The airflow job for sheetload runs every night and is immediately followed by a sheetload-specific dbt run)

### If you need data in Periscope but...

- [ ] Do **NOT** need to link it to other data
    * [ ]  Submitter please follow steps in [CSV Upload Periscope](https://doc.periscopedata.com/article/csv-upload)
    * [ ]  Submitter to close issue if this data will not be used again. 

---

- [ ] **DO** need to link it to other data AND this is a one-off analysis (Boneyard)
    * [ ] Submitter to put spreadsheet data into a new file into the [Sheetload > Boneyard GDrive Folder](https://drive.google.com/open?id=1NdA5CDy2kT653qUdqtCiq_RkmRa-LKqs).
    * [ ] Submitter to share it with the required service account - [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)
    * [ ] Submitter to make an MR to [this file](https://gitlab.com/gitlab-data/analytics/blob/master/extract/sheetload/boneyard/sheets.txt) adding the new sheet in alphabetical order. 
    * [ ] Submitter to assign MR to member of the Data Team
    * [ ] Data Team member to check file name and sheet names to match: The file will be located and loaded based on its name `boneyard.<table_name>`. The names of the sheets shared with the runner must be unique and in the `<file_name>.<tab_name>` format.
    * [ ] Data Team member to merge update after validation of data and MR
    * [ ] Submitter to wait 6 to 8 hours for data to become available (runs 4x per day), or if urgent to ask Data Engineer to trigger job
    * [ ] Submitter to query in Periscope for table: ``` SELECT * FROM boneyard.[new-dbt-model-name] LIMIT 10 ```.

---

- [ ] **DO** need to link it to other data AND want this to be repeatable (Analytics)
    * [ ]  Data Team member to put spreadsheet data into a new file into [Sheetload > Sheetload GDrive Folder](https://drive.google.com/drive/folders/1F5jKClNEsQstngbrh3UYVzoHAqPTf-l0).
    * [ ]  Data Team member to share it with the required service account - [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)
    * [ ]  Data Team member to check file name and sheet names to match: The file will be located and loaded based on its name `sheetload.<table_name>`. The names of the sheets shared with the runner must be unique and in the `<file_name>.<tab_name>` format
    * [ ]  Data Team member to create MR to add this sheet to be pulled in by Sheetload that combines the steps taken in the following MR examples:
           * [ ] Edit the sheets.txt file (Ex: https://gitlab.com/gitlab-data/analytics/merge_requests/1633/diffs)
           * [ ] Edit the schema.yml, sources.yml, and add a new file for the base model (Ex: https://gitlab.com/gitlab-data/analytics/merge_requests/1634/diffs)
    * [ ]  Data Team member to run the following CI Jobs on the MR: 
           * [ ] clone_raw
           * [ ] sheetload
           * [ ] specify_model
    * [ ]  Data Analyst to assign MR of dbt model to Data Engineer team (iterate until model is complete).
    * [ ]  Data Team project maintainers/owners to merge in dbt models 
    * [ ]  If not urgent, data will be availble within 24 hours. If urgent, Data Engineer to run full refresh and inform when available.
    * [ ]  Submitter to query in Periscope for table: ``` SELECT * FROM [new-dbt-model-name] LIMIT 10 ```. 

