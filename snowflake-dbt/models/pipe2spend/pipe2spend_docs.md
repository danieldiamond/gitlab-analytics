{% docs pipe2spend_consolidated %}
This is the consolidated view of Pipe to Spend. Each row represents the pipe to spend for a single month for a specific pipe type. In looker the Pipe to Spend is counting when rows are aggregated, but the pipe to spend for the single row is pre-calculated in case it is needed. 

The pipe CTE filters by New Business and Add-On Business. It also removes sources that are "Web Direct". This is because source came through the web portal, which is often the free version of the site, but it really means it was not sales-assisted. It also filters to have June 2018 because Bizible was started in July. We coalesce sales qualified, sales accepted, and close data months together because there were some upstream data integrity issues where the sales accepted and sales qualified dates weren't being set. This ensures that the opportunity IACV is still being included in the pipe and Marketing gets credit for what they're supposed to. 

The demand_advertising_spend CTE is where the spend attribution occurs. Any attribution not specified will appear as unknown and will need to be allocated in a new MR. It also filters for the 6120 account code and for amounts after March 2018. Since the spend from 3 months prior is applied for pipe to spend, we only care about spend 3 months from when the attribution was started (July 2018), which is April 2018.

The field_events_spend CTE simply sums the amount in 6130.

The spend, headcount, and pipe is joined together to present the simplified view.
{% enddocs %}

{% docs pipe2spend_col_month %}
Represents the month for the calculation.
{% enddocs %}


{% docs pipe2spend_col_pipe_name %}
Current options are Social, Field Event, Organic Search, Google Adwords, Terminus, Paid Sponsorship, Web Referral, Unknown, Web Direct, Paid Social, Email.
{% enddocs %}


{% docs pipe2spend_col_pipe_type %}
This is either Paid or Organic.
{% enddocs %}


{% docs pipe2spend_col_pipe_iacv %}
This is the sum of the IACV for the relevant opportunities.
{% enddocs %}


{% docs pipe2spend_col_headcount %}
This is the count of people for each attribution type.
{% enddocs %}


{% docs pipe2spend_col_salary_per_month %}
This is pegged at 100k per person.
{% enddocs %}


{% docs pipe2spend_col_spend %}
The total spend based on the pipe type.
{% enddocs %}


{% docs pipe2spend_col_pipe_to_spend %}
The precalculated pipe to spend ratio.
{% enddocs %}

