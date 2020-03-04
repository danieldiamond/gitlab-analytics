{%- macro product_category(product_column, output_column_name = 'product_category') -%}

CASE  WHEN lower({{product_column}}) LIKE 'githost%' THEN 'GitHost'
      WHEN {{product_column}} IN ('#movingtogitlab', 'File Locking', 'Payment Gateway Test', 'Time Tracking', '1,000 CI Minutes') THEN 'Other'
      WHEN lower({{product_column}}) LIKE 'gitlab geo%' THEN 'Other'
      WHEN lower({{product_column}}) LIKE 'basic%' THEN 'Basic'
      WHEN lower({{product_column}}) LIKE 'bronze%' THEN 'Bronze'
      WHEN lower({{product_column}}) LIKE 'ci runner%' THEN 'Other'
      WHEN lower({{product_column}}) LIKE 'discount%' THEN 'Other'
      WHEN lower({{product_column}}) LIKE '%premium%' THEN 'Premium'
      WHEN lower({{product_column}}) LIKE '%starter%' THEN 'Starter'
      WHEN lower({{product_column}}) LIKE '%ultimate%' THEN 'Ultimate'
      WHEN lower({{product_column}}) LIKE 'gitlab enterprise edition%' THEN 'Starter'
      WHEN {{product_column}} IN ('GitLab Service Package', 'Implementation Services Quick Start', 'Implementation Support', 'Support Package',
                                  'Admin Training', 'CI/CD Training', 'GitLab Project Management Training', 'GitLab with Git Basics Training',
                                  'Travel Expenses', 'Training Workshop') THEN 'Support'
      WHEN lower({{product_column}}) LIKE '%quick start with ha%' THEN 'Support'
      WHEN lower({{product_column}}) LIKE 'gold%' THEN 'Gold'
      WHEN {{product_column}} = 'Pivotal Cloud Foundry Tile for GitLab EE' THEN 'Starter'
      WHEN lower({{product_column}}) LIKE 'plus%' THEN 'Plus'
      WHEN lower({{product_column}}) LIKE 'premium%' THEN 'Premium'
      WHEN lower({{product_column}}) LIKE 'silver%' THEN 'Silver'
      WHEN lower({{product_column}}) LIKE 'standard%' THEN 'Standard'
      WHEN {{product_column}} = 'Trueup' THEN 'Trueup'
      ELSE NULL
END AS {{output_column_name}}

{%- endmacro -%}
