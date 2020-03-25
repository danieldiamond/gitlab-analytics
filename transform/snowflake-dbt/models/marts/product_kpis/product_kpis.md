{% docs direct_conversion %}

This models selects all subscriptions which are going to be included in the calculation of [Acquisition's KPI](https://about.gitlab.com/direction/acquisition/). Each row  shows a different subscription and its corresponding Annual Recurring Revenue (ARR). 

This model is the union of 2 models built upstream:
* `saas_direct_conversion`
* `self_managed_direct_conversion`

{% enddocs %}

{% docs free_to_paid %}

This models selects all subscriptions which are going to be included in the calculation of [Conversion's KPI](https://about.gitlab.com/direction/conversion/). Each row  shows a different subscription and its corresponding Annual Recurring Revenue (ARR). 

This model is the union of 2 models built upstream:
* `saas_free_to_paid`
* `self_managed_free_to_paid`

{% enddocs %}
