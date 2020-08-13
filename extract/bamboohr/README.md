### BambooHR Extractor

This extractor queries the BambooHR API for 6 data sets:

* Directory - https://www.bamboohr.com/api/documentation/employees.php 
* Tabular Data - https://www.bamboohr.com/api/documentation/tables.php
    * Compensation
    * EmploymentStatus
    * JobInfo
    * CustomBonus
    * EmergencyContacts
* Custom Report - https://www.bamboohr.com/api/documentation/employees.php
    * We pull a report that maps the employee number to the id

The full JSON response is stored in a single row and column (JSONTEXT) with the insertion timestamp in a separate column (UPLOADED_AT) in a Snowflake table in `raw.bamaboohr.<object_name>`.

The dag that runs the job is in `/dags/bamboohr_extract.py`

### Adding new Bamboo HR Tabular data

#### Create Table Command
Execute following command for creating new table in RAW database
```sql
CREATE OR REPLACE TABLE raw.bamboohr.<table_name> (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);
```

#### Initial Load
For the initial DAG run set Airflow Variable `BAMBOOHR_SKIP_TEST` with name of the new table. 
This way the table will be excluded from extraction test that would fail the Dag Run and prevent new data from loading.
Variable can be removed after initial successful run