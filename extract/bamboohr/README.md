### BambooHR Extractor

This extractor queries the BambooHR API for 4 data sets:

* Directory - https://www.bamboohr.com/api/documentation/employees.php 
* Tabular Data - https://www.bamboohr.com/api/documentation/tables.php
    * Compensation
    * EmploymentStatus
    * JobInfo

The full JSON response is stored in a single row and column (JSONTEXT) with the insertion timestamp in a separate column (UPLOADED_AT) in a Snowflake table in `raw.bamaboohr.<object_name>`.

The dag that runs the job is in `/dags/bamboohr_extract.py`


#### Create Table Command
  
```sql
create or replace table bamboohr.employmentstatus (
jsontext variant,
uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);
```
