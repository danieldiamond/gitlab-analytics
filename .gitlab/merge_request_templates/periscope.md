## Periscope Dashboard Checklist

<!--
Please complete all items. Ask questions in the #data slack channel
--->

**Dashboard Link**:
`WIP:` should be in the title and it should be in the `WIP` topic

**Dashboard Name**:

**Original Issue Link**:
<!--
If none, please include a description of the problem to be solved. 
--->

**Editor Slack Handle**: @`handle`

### DRI/Prioritization Owner Checklist

The DRI/prioritization owner can be found [here](https://about.gitlab.com/handbook/business-ops/data-team/#data-support-per-organization)

* Review Items
   * [ ] Does the dashboard provide the data requested?
   * [ ] Is the data in the dashboard correct?
   * [ ] Does the data align with the existing single source of truth and across applicable reporting in Periscope and/or Google Sheets?

### Update to Existing Dashboard Submitter Checklist

* Review Items
   * [ ] `WIP:` should be in the title of the chart being added or updated
   * [ ] Has the DRI/Prioritization Owner approved the update?
   * [ ] Were updates made in accordance with the SQL style guide and Periscope visualization best practices included in the New Dashboard Submitter Checklist?
   * [ ] Have you evangelized the updates in the #data channel or other relevant slack channels?
   * [ ] Remove `WIP:` from the title of the chart being added or updated
   * [ ] Close this MR

### New Dashboard Submitter Checklist
* Review Items
   * [ ] `WIP:` should be in the title of the dashboard and the dashboard should be added to the `WIP` topic
   * [ ] SQL formatted using [GitLab Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)
   * [ ] Python / R reviewed for content, formatting, and necessity, if relevant (Did you write Python or R?)
   * [ ] Filters, if relevant (Did you use filters?)
   * [ ] Current month (in-progress) numbers and historical numbers are in separate charts  (If today is July 15, July should not be in your charts.)- [Here's how to do it](https://about.gitlab.com/handbook/business-ops/data-team/periscope/#filter-out-current-month-in-dashboard-queries).
   * [ ] Drill Down Linked, if relevant
   * [ ] Overview/KPI/Top Level Performance Indicators are cross-linked to the handbook
   * [ ] Section Label (Text box) before more granular metrics 
   * [ ] [Topics](https://doc.periscopedata.com/article/organizing-dashboards-with-topics) (Periscope-speak for Categories) added
   * [ ] Permissions reviewed
   * [ ] Visualization Titles changed to Autofit, if relevant
   * [ ] Axes labeled, if relevant
   * [ ] Numbers (Currencies, Percents, Decimal Places, etc) cleaned, if relevant
   * [ ] If using a date filter, set an appropriate length. Most common is 365 days. - [Here's how to do it](https://about.gitlab.com/handbook/business-ops/data-team/periscope/#working-with-date-range-filters)
   * [ ] Chart description for each chart, linking to the handbook definitions where possible
   * [ ] Legend for each of the charts is clear
   * [ ] Text Tile for "What am I looking at?" and more detailed information, leveraging hyperlinks instead of URLs
   * [ ] Tooltips are used where appropriate and show relevant values
   * [ ] Request approval from stakeholder if applicable
   * [ ] Assign to reviewer on the data team. Use `@gitlab-data` on the dashboard if you don't know who to assign to  

* Housekeeping
  - [ ] Assigned to a member of the data team
  - [ ] Allocated to milestone per review time request
  - [ ] Labels and Points Allocated

### New Dashboard Reviewer Checklist
* Review Items
   * [ ] If dashboard, re-evaluation, first take off official badge. 
   * [ ] SQL formatted using [GitLab Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)
   * [ ] Python / R reviewed for content, formatting, and necessity, if relevant (Did you write Python or R?)
   * [ ] Filters, if relevant (Did you use filters?)
   * [ ] Current month (in-progress) numbers and historical numbers are in separate charts  (If today is July 15, July should not be in your charts.)- [Here's how to do it](https://about.gitlab.com/handbook/business-ops/data-team/periscope/#filter-out-current-month-in-dashboard-queries).
   * [ ] Drill Down Linked, if relevant
   * [ ] Overview/KPI/Top Level Performance Indicators are cross-linked to the handbook
   * [ ] Section Label (Text box) before more granular metrics 
   * [ ] [Topics](https://doc.periscopedata.com/article/organizing-dashboards-with-topics) (Periscope-speak for Categories) added
   * [ ] Permissions reviewed
   * [ ] Visualization Titles changed to Autofit, if relevant
   * [ ] Axes labeled, if relevant
   * [ ] Numbers (Currencies, Percents, Decimal Places, etc) cleaned, if relevant
   * [ ] If using a date filter, set an appropriate length. Most common is 365 days. - [Here's how to do it](https://about.gitlab.com/handbook/business-ops/data-team/periscope/#working-with-date-range-filters)
   * [ ] Chart description for each chart, linking to the handbook definitions where possible
   * [ ] Legend for each of the charts is clear
   * [ ] Text Tile for "What am I looking at?" and more detailed information, leveraging hyperlinks instead of URLs
   * [ ] Tooltips are used where appropriate and show relevant values
   * [ ] If this is a dashboard re-evaluation, check with the business logic owners if the new definitions are correct. 
   * [ ] Remove `WIP:` from title
   * [ ] Remove from `WIP` topic
   * [ ] Add approval badge
   * [ ] Request approval from stakeholder if applicable
   * [ ] Assign to reviewer on the data team. Use `@gitlab-data` on the dashboard if you don't know who to assign to  

### Submitter 
   * [ ] Post in #data channel in Slack
   * [ ] Link to Periscope Charts/Dashboards from the Handbook. Every Periscope Dashboard should be linked from the Handbook **at least once**. All performance indicators can be [embedded in the handbook](https://about.gitlab.com/handbook/business-ops/data-team/periscope/#embedding-periscope-charts-in-the-handbook) except those that are [not public](https://about.gitlab.com/handbook/values/#not-public). 
   * [ ] Close this MR

/label ~Reporting ~Periscope ~Review