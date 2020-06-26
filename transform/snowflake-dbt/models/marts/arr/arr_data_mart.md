{% docs arr_data_mart %}

Data mart to explore ARR. Annual Recurring Revenue (ARR) is a forward looking metric that indicates how much recurring revenue GitLab expects to generate over the next 12 months. For example, the ARR reported for January 2020 would indicate how much recurring revenue is expected to be generated from February 2020 through January 2021.

Charges_month_by_month CTE:

This CTE amortizes the MRR and ARR by month over the effective term of the subscription. There are 4 subscription statuses in Zuora: active, cancelled, draft and expired. The Zuora UI reporting modules use a filter of WHERE subscription_status NOT IN ('Draft','Expired') which is also applied in this query. Please see the column definitions for additional details.

Sample queries: coming soon

{% enddocs %}

{% docs arr_data_mart)incr %}

Daily snapshots of arr_data_mart, calculated starting from 2020-05-01.

{% enddocs %}