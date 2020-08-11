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
* [ ]  Provide information on what data you want exported. If the query needs to be written still, please indicate. (Table name, Columns, SQL statements, etc)
* [ ]  Provide a file name for the file. File Name: ___________ 
* [ ]  Select the type of format you would like the data exported in: 
  - [ ] TSV (tab-separated values)
  - [ ] CSV (comma-separated values)
* [ ]  Does this data contain `RED` or `ORANGE` data as defined in [GitLab's data classification policy?](https://about.gitlab.com/handbook/engineering/security/data-classification-policy.html#data-classification-levels)
  - [ ] Yes (You will need to already have [approved access](https://about.gitlab.com/handbook/business-ops/it-ops-team/access-requests/) to this data) 
  - [ ] No (I verify that this data that will be extracted does not contain sensitive information.)
* [ ]  How soon do you need this data ? Timeline: _________
* [ ]  cc: @gitlab-data to have this issue prioritized and assigned.  

  ### Reviewer Checklist 
* [ ]  Review the Submitter Checklist. 
* [ ]  Check if the data requested. If it is and the requester does not have access, close the issue and comment : "This request has been denied due to access to the information being requested." 
* [ ]  If not, continue with the steps below: 
  - [ ] Run the query in Snowflake. 

#### Small Data Set
* [ ] Export the file in the requested format using the Snowflake UI's export functionality.


#### Large Data Set
* [ ] Load the dataset in the requested format to the `"RAW"."PUBLIC".SNOWFLAKE_EXPORTS` stage by running:

```
COPY INTO @"RAW"."PUBLIC".SNOWFLAKE_EXPORTS/<identifiable_folder_name>/
FROM (<query>)
FILE_FORMAT = (TYPE = {CSV | JSON | PARQUET} <other format options>) 
```

Replace the folder name with something unique and recognizable, such as the name of the issue.  Make sure to [add format options](https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#syntax) to get the data to be formatted as requested.  This is especially important with the CSV format for escaping records and defining delimiters.

By default, the command will create multiple files in GCS.  To just create one file, add the `single = true` option at the end of the command.

After the dataset has been copied into the GCS stage, it can be downloaded from gcs in the [`snowflake_exports` bucket in the `gitlab-analysis` project]().  Follow the 


#### Sharing
* For Sensitive Data
  - [ ] Encrypt the data into zip file (use `zip -er`)
  - [ ] Share the file's password with the submitter over a secure channel separate from the channel you will use to send the file.  [One time secret](https://onetimesecret.com/) may be a good option to share passwords, just make sure to not put in any context with the password. 
* [ ] Share the file with the submitter over a secure channel
* [ ] Reassign this issue to the Submitter for verification of receipt 
* [ ] Once the submitter has verified receipt, delete the file from your computer
