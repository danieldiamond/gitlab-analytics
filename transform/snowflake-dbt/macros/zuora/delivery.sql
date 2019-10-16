{%- macro delivery(product_category_column, output_column_name = 'delivery') -%}

CASE 
  WHEN {{product_category_column}} IN ('Gold', 'Silver', 'Bronze')
    THEN 'SaaS'
  WHEN {{product_category_column}} IN ('Ultimate', 'Premium', 'Starter')
    THEN 'Self-Managed'
  WHEN {{product_category_column}} IN ('Basic', 'GitHost', 'Other', 'Plus', 'Standard', 'Support', 'Trueup')
    THEN 'Others'
  ELSE NULL
END AS {{output_column_name}}

{%- endmacro -%}
