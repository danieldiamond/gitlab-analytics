with account as (
		select * from {{ ref('account') }}
)

SELECT row_number() OVER (
                          ORDER BY sfdc_account_id) AS id,
       COALESCE(sfdc_account_id, 'Unknown') as sfdc_account_id,
       COALESCE(name, 'Unknown') as name,
       COALESCE(industry, 'Unknown') as industry,
       COALESCE(type, 'Unknown') as type,
       COALESCE(Sales_Segmentation__c, 'Unknown') as sales_segmentation,
       COALESCE(ultimate_parent_Sales_Segmentation, 'Unknown') as ultimate_parent_sales_segmentation,
       COALESCE(ultimate_parent_name, 'Unknown') as ultimate_parent_name
FROM account 