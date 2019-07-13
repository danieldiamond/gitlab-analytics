{% docs zendesk_ticket_sla %}

This value calculates SLA status information for each ticket. The criteria for meeting an SLA is as follows:
- Urgent tickets have an SLA of 30 minutes with a time period of 24 hours/7 days a week
- High priority tickets have an SLA of 4 hours with a time period of 24 hours/5 days a week. On Fridays, the goal SLA is adjusted to take into account the weekend for tickets received with less than 4 hours left in the work day. Similarly, tickets received on Saturday or Sunday have an SLA clock beginning Monday morning.
- Normal priority tickets have an SLA of 8 hours with a time period of 24 hours/5 days a week. On Fridays, the goal SLA is adjusted to take into account the weekend for tickets received with less than 8 hours left in the work day. Similarly, tickets received on Saturday or Sunday have an SLA clock beginning Monday morning.
- Low priority tickets have an SLA of 24 hours with a time period of 24 hours/5 days a week. On Fridays, the goal SLA is adjusted to take into account the weekend for tickets received with less than 24 hours left in the work day. Similarly, tickets received on Saturday or Sunday have an SLA clock beginning Monday morning.

The time used to decide whether SLA was met is the first non-null value of first reply time (for tickets with public responses), first resolution time, or final resolution time. The final SLA metric calculation is as follows:

 `Number of Times SLA met / Total Tickets SLA was applicable`.

The operating metric can be found [here](https://about.gitlab.com/handbook/finance/operating-metrics/#service-level-agreement-sla). SLA's can be found [here](https://about.gitlab.com/support/).

{% enddocs %}
