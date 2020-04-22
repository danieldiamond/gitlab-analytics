{% docs dim_customers %}
Dimensional customer table representing all currently existing and historical customers
as defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer)

Columns explained:
* customer_id - account id from SFDC identifing the customer
* customer_name - account name from SFDC
* customer_country - billing country as in SFDC
* ultimate_parent_account_id - ultimate parent account id
* ultimate_parent_account_name - parent account name
* ultimate_parent_account_country - country of where the parent account is
* is_deleted - flag indicating if account has been deleted
* merged_to_account_id - for deleted accounts this is the SFDC account they were merged to

{% enddocs %}