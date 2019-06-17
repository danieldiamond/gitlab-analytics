## SheetLoad - Spreadsheet & CSV Loader Utility

<img src="https://gitlab.com/meltano/analytics/uploads/d90d572dbc2b1b2c32ce987d581314da/sheetload_logo.png" alt="SheetLoadLogo" width="600"/>

Spreadsheets can be loaded into the our data warehouse using `extract/sheetload/sheetload.py`. 
Local CSV files can be loaded as well as spreadsheets in Google Sheets or files from GCS.
A Google Sheet or local CSV will only be loaded if there has been a change between the current and existing data in the DW. 
GCS files will always force a full reload.

Loading a CSV:

  - The naming format for the file must be `<schema>.<table>.csv`. This pattern is required and will be used to create/update the table in the DW.
  - Run the command `python3 sheetload.py csv <path>/<to>/<csv>/<file_name>` Multiple paths can be used, use spaces to separate.
  - The script will assume no compression, if there is compression use the `--compression` flag and a string describing the type of compression, i.e. `gzip`
  - Logging from the script will tell you table successes/failures and the number of rows uploaded to each table.


Loading a Google Sheet:

  - Share the sheet with the required service account - [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)
  - Files are stored in the [Sheetload folder](https://drive.google.com/open?id=1F5jKClNEsQstngbrh3UYVzoHAqPTf-l0)
  - All tables will automatically be uploaded to the `sheetload` schema
  - The file will be located and loaded based on its name. The names of the sheets shared with the runner must be unique and in the `<file_name>.<tab_name>` format
  - List the names in a newline delimited txt file
  - Run the command `python3 sheetload.py sheets <path/to/txt/file> <snowflake|postgres>`
  - Logging from the script will tell you table successes/failures and the number of rows uploaded to each table.


Loading a CSV from GCS:

  - Put it in a bucket in the gitlab-analysis project
  - Make sure that the runner account has access (it should by default)
  - the name of the table will be the first part of the file name, for instance `data.csv.gz` will be in the table `data`
  - Run the command `python3 sheetload.py gcs --bucket <bucket_name> --destination <snowflake|postgres> <file_name1> <file_name2> ...`
  - This command has multiple default params: `schema` (the target schema) = `sheetload`, `compression` (what compression is used) = `gzip`
  - Sheetload will then download the file and iterate through it, 15000 records at a time and upload them. 

Further Usage Help:

  - See [the sheetload dag](https://gitlab.com/gitlab-data/analytics/blob/master/dags/extract/sheetload.py) for a real world example of usage
  - Run the following command(s) for additional usage info `python3 extract/historical/sheetload.py <csv|sheets|gcs> -- --help`

### Behavior

Read more [in the handbook](https://about.gitlab.com/handbook/business-ops/data-team/#using-sheetload).

