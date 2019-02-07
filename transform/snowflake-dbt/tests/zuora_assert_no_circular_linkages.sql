--no circular subscriptions

with base as (
select subscription_name_slugify, c.value::string as subscriptions_in_lineage
from {{ref('zuora_subscription_lineage')}},
lateral flatten(input =>split(lineage, ',')) C
WHERE c.index != 0
)
SELECT *
FROM base
WHERE subscription_name_slugify = subscriptions_in_lineage

