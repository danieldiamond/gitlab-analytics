{%- macro cost_category(account_number, account_full_name,
 output_column_name = 'cost_category') -%}

CASE WHEN ({{account_number}} = '5012' AND LOWER({{account_full_name}}) = 'business gifts cost of sales')
     OR   ({{account_number}} = '5045' AND LOWER({{account_full_name}}) = 'online marketing cost of sales')
     OR   ({{account_number}} = '5047' AND LOWER({{account_full_name}}) = 'other general expense cost of sales')
     OR   ({{account_number}} = '5048' AND LOWER({{account_full_name}}) = 'other office expense cost of sales')
     OR   ({{account_number}} = '6000' AND LOWER({{account_full_name}}) = 'expenses')
     OR   ({{account_number}} = '6012' AND LOWER({{account_full_name}}) = 'business gifts')
     OR   ({{account_number}} = '6059' AND LOWER({{account_full_name}}) = 'software - expensed')            THEN 'Non-Headcount'
     WHEN RIGHT({{account_number}},3)  IN (000,001,009,010,011,012,014,022,029,045,047,048,058,059,067,077) THEN 'Headcount'
     ELSE 'Non-Headcount'
END AS {{output_column_name}}

{%- endmacro -%}
