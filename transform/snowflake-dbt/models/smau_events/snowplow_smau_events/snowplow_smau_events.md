{% docs plan_snowplow_smau_events %}

This model provides a summary of relevant activation events for Plan Stage coming from snowplow frontend events (pageviews and events). A summary of all activation events is at the moment defined in this [issue](https://gitlab.com/gitlab-org/telemetry/issues/48).

From snowplow database, at the moment we track the following events:

* board_viewed
* epic_list_viewed
* epic_viewed
* issue_list_viewed
* issue_viewed
* milestones_list_viewed
* milestone_viewed
* notification_settings_viewed
* personal_issues_viewed
* roadmap_viewed
* todo_viewed

{% enddocs %}
