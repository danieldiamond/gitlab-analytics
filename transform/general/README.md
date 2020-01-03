## Cleaning Slack Data

`clean_slack_data.R` is a simple helper script to clean GitLab slack statistics data. This currently involves a number of manual steps to ensure monthly exports of data are available in Periscope.

1. Open Slack and navigate to the Analytics pop-up window.
1. Change the dropdown to All Time and Export.
1. The cleaning script expects `/scratch/slack_stats/` to exist in your user directory, as well as `/data/raw` and `/data/clean`. These can be modified to suit your local environment.
1. Move the exported csv to the `/data/raw/` directory.
1. Execute `clean_slack_data.R`. This will generate a csv file "sheetload_gitlab_slack_data_(date_of_execution).csv".
1. In the Sheetload folder in Google Drive, find `sheetload.gitlab_slack_stats`. Import the generated csv as a replacement for the current sheet.
1. Update the GitLab Slack dashboard in Periscope to reflect the date of updated data.
