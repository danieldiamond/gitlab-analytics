{% docs sessionized_saas_funnel_90 %}

[Inspiration for this model](https://snowplowanalytics.com/blog/2016/03/16/introduction-to-event-data-modeling/#workflows)

Our summary model aggregates the event-level data down to a funnel level. Each row represents a different session that entered this funnel.

The SaaS Funnel hash the following steps:

1. Click on one of the SaaS packages [from the pricing page](about.gitlab.com/pricing), event: `subscription_funnel_start_page`
1. Create a Saas subscription, event: `subscription_funnel_start_page`


{% enddocs %}
