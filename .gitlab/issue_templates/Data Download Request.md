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
* [ ]  Provide information on what data you want exported. (Table name, Columns, SQL statements, etc)
* [ ]  Provide a file name for the file. File Name: ___________ 
* [ ]  Select the type of format you would like the data exported in: 
  - [ ] TSV (tab-separated values)
  - [ ] CSV (comma-separated values)
* [ ]  Does this data contain `RED` or `ORANGE` data as defined in [GitLab's data classification policy?](https://about.gitlab.com/handbook/engineering/security/data-classification-policy.html#data-classification-levels)
  - [ ] Yes (If you don't currently have access to this data you will need to submit an approved an access request) 
  - [ ] No (I verify that this data that will be extracted does not contain sensitive information.)
* [ ]  How soon do you need this data ? Timeline: _________
* [ ]  cc: @gitlab-data to have this issue prioritized and assigned.  

  ### Reviewer Checklist 
* [ ]  Review the Submitter Checklist. 
* [ ]  Check if the data requested is sensitive. If it is, close the issue and comment : "This request has been denied due to access to the information being requested." 
* [ ]  If not, continue with the steps below: 
  - [ ] Run the query in Snowflake. 
  - [ ] Export the file in the requested format. 
  - Sensitive Data
    - [ ] Encrypt the data into zip file (use `zip -er`)
    - [ ] share the file's password with the submitter over a secure channel separate from the channel you will use to send the file
  - [ ] Share the file with the submitter over a secure channel
  - [ ] Reassign this issue to the Submitter for verification of receipt 
  - [ ] Once the submitter has verified receipt delete the file from your computer
