## Request for data extraction

<!--
Please complete all items. Ask questions in the #data slack channel
--->

**Original Issue Link**:
<!--
If none, please include a description
--->

**Editor Slack Handle**: @`handle`

**Business Use Case** (Please explain what this data will be used for and appropriate data filters): 


### Submitter Checklist
* [ ]  Provide link to dashboard that you wanted to export from Periscope that was greater than 5MB. Link: _____ 
* [ ]  Provide information on what data you want exported. (Table name, SQL statements, etc)
* [ ]  Provide a file name for the file. File Name: ___________ 
* [ ]  Select the type of format you would like the data exported in: 
  - [ ] TSV (tab-separated values)
  - [ ] CSV (comma-separated values)
* [ ]  Does this data contain sensitive information? 
  - [ ] Yes (this request will be denied.) 
  - [ ] No (I verify that this data that will be extracted does not contain sensitive information.)
* [ ]  How soon do you need this data ? Timeline: _________
* [ ]  cc: @gitlab-data to have this issue prioritized and assigned.  

  ### Reviewer Checklist 
* [ ]  Review the Submitter Checklist. 
* [ ]  Check if the data requested is sensitive. If it is, close the issue and comment : "This request has been denied due to sensitive information being requested." 
* [ ]  If not, continue with the steps below: 
  - [ ] Run the query in Snowflake. 
  - [ ] Export the file in the requested format. 
  - [ ] Mark issue as confidential. 
  - [ ] Drop the file into this issue. 
  - [ ] Reassign this issue to the Submitter. 
  - [ ] Close the issue. 