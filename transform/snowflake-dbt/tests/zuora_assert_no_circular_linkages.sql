--no circular subscriptions

select *, c.value::string as subscriptions_in_lineage
from {{ref('zuora_subscription_lineage')}},
lateral flatten(input=>split(lineage, ',')) C
where subscription_name_slugify = subscriptions_in_lineage
AND subscription_name_slugify != 'a-s00003735'
AND lineage NOT LIKE 'a-s00003735%'