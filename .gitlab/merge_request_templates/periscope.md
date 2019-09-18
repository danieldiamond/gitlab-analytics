## Periscope Dashboard Checklist

<!--
Please complete all items. Ask questions in #data
--->

**Dashboard Link**:
`WIP:` should be in the title and it should be in the `WIP` topic

**Dashboard Name**:

**Original Issue Link**:
<!--
If none, please include a description
--->

**Editor Slack Handle**: @`handle`

### DRI/Prioritization Owner Checklist

The DRI/prioritization owner can be found [here](https://about.gitlab.com/handbook/business-ops/data-team/#data-support-per-organization)

* Review Items
   * [ ] Does the dashboard provide the data requested?
   * [ ] Is the data in the dashboard correct?
   * [ ] Does the data align with the existing single source of truth and across applicable reporting in Periscope and Google Sheets?

### Submitter Checklist
* Review Items
   * [ ] SQL formatted using [GitLab Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)
   * [ ] Python / R reviewed for content, formatting, and necessity
   * [ ] Filters, if relevant
   * [ ] Current month (in-progress) numbers and historical numbers are in separate charts
   * [ ] Drill Down Linked, if relevant
   * [ ] Overview/KPI/Top Level Metrics cross-linked
   * [ ] Section Label before more granular metrics
   * [ ] Topics added
   * [ ] Permissions reviewed
   * [ ] Viz Titles changed to Autofit, if relevant
   * [ ] Axes labeled, if relevant
   * [ ] Numbers (Currencies, Percents, Decimal Places, etc) cleaned, if relevant
   * [ ] If using a date filter, set an appropriate length. Most common is 365 days.
   * [ ] Chart description for each chart, linking to Metrics definitions where possible
   * [ ] Legend is clear
   * [ ] Text Tile for "What am I looking at?" and more detailed information, leveraging hyperlinks instead of URLs
   * [ ] Tooltips are used where appropriate and show relevant values
   * [ ] Request approval from stakeholder if applicable
   * [ ] Assign to reviewer on the data team

* Housekeeping
  - [ ] Assigned to a member of the data team
  - [ ] Allocated to milestone per review time request
  - [ ] Labels and Points Allocated

### Reviewer Checklist
* Review Items
   * [ ] SQL formatted using [GitLab Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)
   * [ ] Python / R reviewed for content, formatting, and necessity
   * [ ] Filters, if relevant
   * [ ] Current month (in-progress) numbers and historical numbers are in separate charts
   * [ ] Drill Down Linked, if relevant
   * [ ] Overview/KPI/Top Level Metrics cross-linked
   * [ ] Section Label before more granular metrics
   * [ ] Topics added
   * [ ] Permissions reviewed
   * [ ] Viz Titles changed to Autofit, if relevant
   * [ ] Axes labeled, if relevant
   * [ ] Numbers (Currencies, Percents, Decimal Places, etc) cleaned, if relevant
   * [ ] If using a date filter, set an appropriate length. Most common is 365 days.
   * [ ] Chart description for each chart, linking to Metrics definitions where possible
   * [ ] Legend is clear
   * [ ] Text Tile for "What am I looking at?" and more detailed information, leveraging hyperlinks instead of URLs
   * [ ] Tooltips are used where appropriate and show relevant values
   * [ ] Remove `WIP:` from title
   * [ ] Remove from `WIP` topic
   * [ ] Add approval badge
   * [ ] Request approval from stakeholder/business partner if applicable
   * [ ] Assign back to submitter for closing or merge if good to go

### Submitter
   * [ ] Post in #data channel in Slack
   * [ ] Close this MR

/label ~Reporting ~Periscope ~Review