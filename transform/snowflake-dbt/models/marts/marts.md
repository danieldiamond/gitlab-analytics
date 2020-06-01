{% docs arr_monthly_reporting_mart %}

Data mart to explore ARR. Annual Recurring Revenue (ARR) is a forward looking metric that indicates how much recurring revenue GitLab expects to generate over the next 12 months. For example, the ARR reported for January 2020 would indicate how much recurring revenue is expected to be generated from February 2020 through January 2021.

Sample queries: coming soon

{% enddocs %}

{% docs invoice_charges_mart %}

This model constructs a history of all changes to charges on subscriptions by leveraging Invoice information.

Similar to the relationship between Subscription Name and Subscription ID,  Charge Number is the unique identifier for a Rate Plan Charge that is inclusive of multiple unique Rate Plan Charge IDs. Renewals, increases/decreases in seat count, and changes to effective start and end dates are all tracked against the Charge Number, with specific changes incrementing the Rate Plan Charge ID.

Similar to the relationship between Subscription Name and Subscription Version, each time a Rate Plan Charge is amended a new version will be created. However, a new Rate Plan Charge ID does not necessitate a new Rate Plan Charge Version, such as when additional charges are added to a rate plan or terms and conditions change.

Finally, Segment is the key identifier for when the dollar amount on a Charge Number was changed.

Putting it all together, the end result is a model with one row for every Charge Number, Segment, and Version of Rate Plan Charges that were invoiced, along with associated metadata from the RatePlanCharge, InvoiceItem, and Invoice objects.

{% enddocs %}
