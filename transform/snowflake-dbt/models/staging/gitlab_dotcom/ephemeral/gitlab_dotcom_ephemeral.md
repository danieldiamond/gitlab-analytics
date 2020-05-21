{% docs gitlab_dotcom_issues_linked_to_sfdc_account_id %}

The final table is an auxiliary mapping table where each row is a unique tuple of a `noteable_type` (issue or epic), `noteable_id` and an `sfdc_account_id`, which means that the SFDC account `account_id` has been referenced in the issue description of the noteable with `noteable_id` which is part of one of the Gitlab's projects.

This model extracts and flattens the salesforce IDs from the issue description. These IDs could be coming from accounts, opportunities or other objects from Salesforce. Once these SFDC ID are flattened, we join the flattened table on several SFDC base models in order to find the nature of the ID :

* account
* contact
* leads
* opportunity

We eventually normalise the output table to keep the `account_id` .

We reiterate the same process with zendesk tickets (extracting zendesk ticket id, linking them to `zendesk_tickets_xf` and then associating them to a `sfdc_account_id`) 

{% enddocs %}


{% docs gitlab_dotcom_notes_linked_to_sfdc_account_id %}

The final table is an auxiliary mapping table where each row is a unique tuple of a `noteable_type` (issue or epic), `noteable_id` and an `sfdc_account_id`, which means that the SFDC account `account_id` has been referenced in the issue description of the noteable with `noteable_id` which is part of one of the Gitlab's projects.

This model flattens the salesforce IDs array extracted in the model `gitlab_dotcom_gitlab_namespaces_notes`. These IDs could be coming from accounts, opportunities or other objects from Salesforce. Once these SFDC ID are flattened, we join the flattened table on several SFDC base models in order to find the nature of the ID :

* account
* contact
* leads
* opportunity

We eventually normalise the output table to keep the `account_id`.

We reiterate the same process with zendesk tickets (extracting zendesk ticket id, linking them to `zendesk_tickets_xf` and then associating them to a `sfdc_account_id`) 


{% enddocs %}
