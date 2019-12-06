{% docs configure_smau_events %}

This model encapsulates all activation events for the Configure Stage as defined in this GitLab [issue](https://gitlab.com/gitlab-org/telemetry/issues/53).

{% enddocs %}

{% docs create_smau_events %}

This model encapsulates all activation events for the create stage as defined in this gitlab [issue](https://gitlab.com/gitlab-org/telemetry/issues/49). It reconciles 2 different data sources (Snowplow and Gitlab) with some common enabling us to calculate Daily/Monthly Active User count for this specific stage.

For more documentation on which event is tracked by each data source for this stage, refer to the 2 upstream models ((snowplow_create_activation_events)[] and (gitlab_create_activation_events)[])

{% enddocs %}

{% docs manage_smau_events %}

This model encapsulates all activation events for the Manage Stage as defined in this GitLab [issue](https://gitlab.com/gitlab-org/telemetry/issues/47).

{% enddocs %}

{% docs monitor_smau_events %}

This model encapsulates all activation events for the Monitor stage as originally defined in this Gitlab [issue](https://gitlab.com/gitlab-org/telemetry/issues/54).

{% enddocs %}

{% docs plan_smau_events %}

This model encapsulates all activation events for stage create as defined in this gitlab [issue](https://gitlab.com/gitlab-org/telemetry/issues/48).

{% enddocs %}

{% docs release_smau_events %}

This model encapsulates all activation events for the Release Stage as defined in this GitLab [issue](https://gitlab.com/gitlab-org/telemetry/issues/52).

{% enddocs %}

{% docs verify_smau_events %}

This model encapsulates all activation events for stage verify as defined in this gitlab [issue](https://gitlab.com/gitlab-org/telemetry/issues/50).

{% enddocs %}
