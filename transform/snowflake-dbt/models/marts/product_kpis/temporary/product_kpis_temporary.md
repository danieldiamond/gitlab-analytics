{% docs saas_direct_conversion %}

This models selects all SaaS subscriptions which are going to be included in the calculation of [Acquisition's KPI](https://about.gitlab.com/direction/acquisition/). Each row  shows a different subscription and its corresponding Annual Recurring Revenue (ARR). 

The ARR is calculated from the `customers_db_charges_xf` model following these steps:
* take all charges that have  the `subscription_version_term_start_date` similar to the `subscription_start_date`
* for all charges, look the `month_interval` up
  * if `month_interval` is lower than 12, `arr = month_intetval * mrr`
  * if `month_interval` is equal or greaer than 12, `arr = 12 * mrr`
  
More details about the subscriptions that are included in the KPI [here](https://about.gitlab.com/direction/acquisition/)

{% enddocs %}

{% docs saas_free_to_paid %}

This models selects all SaaS subscriptions which are going to be included in the calculation of [Conversion's KPI](https://about.gitlab.com/direction/conversion/). Each row  shows a different subscription and its corresponding Annual Recurring Revenue (ARR). 

The ARR is calculated from the `customers_db_charges_xf` model following these steps:
* take all charges that have  the `subscription_version_term_start_date` similar to the `subscription_start_date`
* for all charges, look the `month_interval` up
  * if `month_interval` is lower than 12, `arr = month_intetval * mrr`
  * if `month_interval` is equal or greaer than 12, `arr = 12 * mrr`
  
More details about the subscriptions that are included in the KPI [here](https://about.gitlab.com/direction/conversion/)

{% enddocs %}

{% docs self_managed_direct_conversion %}

This models selects all self-managed subscriptions which are going to be included in the calculation of [Acquisition's KPI](https://about.gitlab.com/direction/acquisition/). Each row  shows a different subscription and its corresponding Annual Recurring Revenue (ARR). 

The ARR is calculated from the `customers_db_charges_xf` model following these steps:
* take all charges that have  the `subscription_version_term_start_date` similar to the `subscription_start_date`
* for all charges, look the `month_interval` up
  * if `month_interval` is lower than 12, `arr = month_intetval * mrr`
  * if `month_interval` is equal or greaer than 12, `arr = 12 * mrr`
  
More details about the subscriptions that are included in the KPI [here](https://about.gitlab.com/direction/acquisition/)

{% enddocs %}

{% docs self_managed_free_to_paid %}

This models selects all self-managed subscriptions which are going to be included in the calculation of [Conversion's KPI](https://about.gitlab.com/direction/conversion/). Each row  shows a different subscription and its corresponding Annual Recurring Revenue (ARR). 

The ARR is calculated from the `customers_db_charges_xf` model following these steps:
* take all charges that have  the `subscription_version_term_start_date` similar to the `subscription_start_date`
* for all charges, look the `month_interval` up
  * if `month_interval` is lower than 12, `arr = month_intetval * mrr`
  * if `month_interval` is equal or greaer than 12, `arr = 12 * mrr`
  
More details about the subscriptions that are included in the KPI [here](https://about.gitlab.com/direction/conversion/)

{% enddocs %}
