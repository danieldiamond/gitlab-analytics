{% docs zendesk_ticket_csat %}

This table displays satisfaction rating scores based on customer survey responses after the ticket is closed as well as additional organizational and ticket metadata such as organization market segment and ticket priority. Tickets with unoffered (not sent a survey) and offered (survey sent but no response given) statuses have been filtered out.

We can calculate the CSA using the following formula: `CSAT = good scores/(good scores + bad scores)`. 

The operating metric can be found [here](https://about.gitlab.com/handbook/finance/operating-metrics/#csat)

{% enddocs %}


{% docs zendesk_ticket_sla %}

This table reflects ticket resolution information for closed tickets including the difference between first and final resolutions in both business and calendar hours. Organizational meta data has been merged in to provide the SFDC ID for retrieving support level. SLA is currently measured on tickets submitted by customers with top Support plans (Premium for Self-managed, Gold for Gitlab.com).The SLA can be calculated as the `Number of Times SLA met / Total Tickets SLA was applicable`.

The operating metric can be found [here](https://about.gitlab.com/handbook/finance/operating-metrics/#service-level-agreement-sla). SLA's can be found [here](https://about.gitlab.com/support/).

{% enddocs %}