{% docs zuora_subscription %}

This table is the base table for Zuora subscriptions.

It removes all subscriptions that are marked as "Exclude from Analysis". It also creates a slug of the subscription name and the renewal subscription name (if it exists) using the user-defined function (UDF) "zuora_slugify". There is a test specifically to validate this.

This table also sets the zuora_renewal_subscription_name_slugify field as an array. This is important because we need to do deduplication downstream and can't have a fan out at this stage. Storing the data as an array enables us to still clean it with the slugify UDF while still having a single row.

#### Zuora Slugify UDF
This function is used to simplify Zuora subscriptions names. Subscription names, by default, are unique. However, they can have basically any character they want in them, including arbitrary amounts of whitespace. The original idea was to truly slugify the name but that wasn't unique enough across subscriptions. We did have an issue where links to renewal subscriptions were not being copied correctly. Creating a slug that simply replaces most characters with hyphens maintained uniqueness but made it easier to reason about how the data was incorrectly formatted. It in the end it may not be wholly necessary, but from a programming perspective it is a bit easier to reason about.

{% enddocs %}


{% docs zuora_account_source %}

The account source table contains information about the customer accounts in your Zuora instance. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/zuora#account)

{% enddocs %}


{% docs zuora_contact_source %}

The contact source table contains info about an account’s point-of-contact. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/zuora#contact)

{% enddocs %}


{% docs zuora_discount_applied_metrics_source %}

The discountAppliedMetrics source table contains info about rate plan charges that use either a discount-fixed amount or discount-percentage charge model. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/zuora#discountappliedmetrics)

{% enddocs %}

{% docs zuora_invoice_item_source %}

The invoiceItem source table contains info about the line items in invoices. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/zuora#invoiceitem)

{% enddocs %}

{% docs zuora_invoice_source %}

The invoice source table contains info about invoices, which are bills to customers. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/zuora#invoice)

{% enddocs %}

{% docs zuora_rateplan_source %}

The ratePlan source table contains info about rate plans, which is a price or collection of prices for services. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/zuora#rateplan)

{% enddocs %}

{% docs zuora_rateplan_charge_source %}

This is the source table for Zuora Rate Plan Charges. [Link to Documentation](https://knowledgecenter.zuora.com/DC_Developers/G_SOAP_API/E1_SOAP_API_Object_Reference/RatePlanCharge)

{% enddocs %}

{% docs zuora_refund_source %}

This is the source table for Zuora Refunds. [Link to Documentation](https://knowledgecenter.zuora.com/DC_Developers/G_SOAP_API/E1_SOAP_API_Object_Reference/Refund)

{% enddocs %}

{% docs zuora_subscription_source %}

The subscription source table contains info about your products and/or services with recurring charges.
[Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/zuora#subscription)

{% enddocs %}
