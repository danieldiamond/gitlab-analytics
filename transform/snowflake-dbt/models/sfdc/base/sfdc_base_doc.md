{% docs sfdc_account_source %}

The account source table contains info about the individual accounts (organizations and persons) involved with your business. This could be a customer, a competitor, a partner, and so on. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#account)

Note: A number of fields prefixed with JB_ and ATAM_ are pulled in as part of [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp). These are cast as fields prefixed with TSP_ in downstream models to distinguish from equivalent "Actual" fields reflecting the current Go-To-Market approach.

{% enddocs %}

{% docs sfdc_bizible_source %}

Bizible generated table pulled via Salesforce. Source of truth for Bizible data.

{% enddocs %}

{% docs sfdc_campaignmember_source %}

The campaign member source table represents the association between a campaign and either a lead or a contact. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_campaignmember.htm)

{% enddocs %}

{% docs sfdc_campaign_source %}

This campaign source table represents and tracks a marketing campaign, such as a direct mail promotion, webinar, or trade show. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_campaign.htm)

{% enddocs %}

{% docs sfdc_contact_source %}

The contact source table contains info about your contacts, who are individuals associated with accounts in your Salesforce instance. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#contact)

{% enddocs %}

{% docs sfdc_execbus_source %}

Custom source table: This table contains executive business review data.

{% enddocs %}

{% docs sfdc_lead_source %}

The lead source table contains info about your leads, who are prospects or potential Opportunities. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#lead)

Note: A number of fields prefixed with JB_ and ATAM_ are pulled in as part of [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp). These are cast as fields prefixed with TSP_ in downstream models to distinguish from equivalent "Actual" fields reflecting the current Go-To-Market approach.

{% enddocs %}

{% docs sfdc_opphistory_source %}

The opportunity history source table represents the stage history of an Opportunity. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_opportunityhistory.htm)

{% enddocs %}

{% docs sfdc_oppline_source %}

The opportunity line item source table represents an opportunity line item, which is a member of the list of Product2 products associated with an Opportunity. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_opportunitylineitem.htm)

{% enddocs %}

{% docs sfdc_oppstage_source %}

The opportunity stage source table represents the stage of an Opportunity in the sales pipeline, such as New Lead, Negotiating, Pending, Closed, and so on. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_opportunitystage.htm)

{% enddocs %}

{% docs sfdc_opp_source %}

The opportunity source table contains info about your opportunities, which are sales or pending deals. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#opportunity)

{% enddocs %}

{% docs sfdc_pov_source %}

This table contains data on the proof of value. Note the API name for this source remained proof_of_concept after renaming of the SFDC custom object.

{% enddocs %}

{% docs sfdc_professional_services_engagement_source %}

This table contains data on the professional services engagement. Note the API name for this source remained statement_of_work after renaming of the SFDC custom object.

{% enddocs %}

{% docs sfdc_quote_source %}

This table contains data on Zuora quotes generated for opportunities. This provides the SSOT mapping between opportunities and subscriptions for quotes where `status = 'Sent to Z-Billing'` and `is_primary_quote = TRUE`
.
{% enddocs %}

{% docs sfdc_recordtype_source %}

The record type source table represents a record type. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_recordtype.htm)

{% enddocs %}

{% docs sfdc_task_source %}

The task source table represents a business activity such as making a phone call or other to-do items. In the user interface, Task and Event records are collectively referred to as activities. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_task.htm)

{% enddocs %}

{% docs sfdc_userrole_source %}

The user role source table represents a user role in your organization. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_role.htm)

{% enddocs %}

{% docs sfdc_user_source %}

The user source table contains info about the users in your organization. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#user)

{% enddocs %}

{% docs sfdc_acct_arch_source %}

This is the source table for the archived Salesforce accounts.

{% enddocs %}

{% docs sfdc_opp_arch_source %}

This is the source table for the archived Salesforce opportunities.

{% enddocs %}

{% docs sfdc_users_arch_source %}

This is the source table for the archived Salesforce users.

{% enddocs %}

{% docs sfdc_oppfieldhistory_source %}

This is the source table for Opportunity Field History.

{% enddocs %}
