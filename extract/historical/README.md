## SheetLoad - Spreadsheet & CSV Loader Utility

<img src="https://gitlab.com/meltano/analytics/uploads/d90d572dbc2b1b2c32ce987d581314da/sheetload_logo.png" alt="SheetLoadLogo" width="600"/>

Spreadsheets can be loaded into the DW (Data Warehouse) using `extract/historical/sheetload.py`. Local CSV files can be loaded as well as spreadsheets in Google Sheets.
A file will only be loaded if there has been a change between the current and existing data in the DW.

Loading a CSV:

  - Start the cloud sql proxy
  - The naming format for the file must be `<schema>.<table>.csv`. This pattern is required and will be used to create/update the table in the DW.
  - Run the command `python3 extract/historical/sheetload.py csv <path>/<to>/<csv>/<file_name>.csv` Multiple paths can be used, use spaces to separate.
  - Logging from the script will tell you table successes/failures and the number of rows uploaded to each table.


Loading a Google Sheet:

  - Share the sheet with the required service account (if being used in automated CI, use the runner service account)
  - Files are stored in the [SheetShift folder on Google Drive](https://drive.google.com/open?id=1F5jKClNEsQstngbrh3UYVzoHAqPTf-l0)
  - The file will be located and loaded based on its name. The names of the sheets shared with the runner must be unique and in the `<schema>.<file_name>.<sheet_mame>` format
  - Run the command `python3 extract/historical/sheetload.py sheet <name>` Multiple names can be used, use spaces to separate.
  - Logging from the script will tell you table successes/failures and the number of rows uploaded to each table.

Further Usage Help:

  - See the CI file (spreadsheet_extractor job) for real world example of usage
  - Run the following command(s) for additional usage info `python3 extract/historical/sheetload.py <csv|sheet> -- --help`

### Behaviour

SheetLoad is designed to make the table in the database a mirror image of what is in the sheet that it is loading from. Whenever SheetLoad detects a change in the source sheet
it will forcefully drop the database table and recreate it in the image of the updated spreadsheet. This means that if columns are added, changed, etc. it will all get reflected
in the database.

