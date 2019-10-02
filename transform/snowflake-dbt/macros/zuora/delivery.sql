{%- macro delivery(product_category_column, output_column_name = 'delivery') -%}

CASE 
  WHEN {{product_category_column}} IN ('Gold', 'Silver', 'Bronze')
    THEN 'GitLab.com'
  WHEN {{product_category_column}} IN ('Ultimate', 'Premium', 'Starter')
    THEN 'self-managed'
  WHEN {{product_category_column}} IN ('Basic', 'GitHost', 'Other', 'Plus', 'Standard', 'Support', 'Trueup')
    THEN 'Others'
END AS {{output_column_name}}

{%- endmacro -%}
