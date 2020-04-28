# Useful links



```
    SELECT
    COALESCE(dim_customers.merged_to_account_id , dim_customers.CRM_ID) AS customer_id,
    subscription_name_slugify,
    dateadd('month',-1,dim_dates.first_day_of_month) as mrr_month,
    mrr
    FROM ANALYTICS.ANALYTICS_STAGING.dim_dates
     INNER JOIN ANALYTICS.ANALYTICS_STAGING.fct_charges ON fct_charges.effective_start_date_id <= dim_dates.date_id
     AND fct_charges.effective_end_date_id > dim_dates.date_id  AND dim_dates.day_of_month=1
    INNER JOIN ANALYTICS.ANALYTICS_STAGING.fct_invoice_items_agg ON fct_charges.charge_id = fct_invoice_items_agg.charge_id
    INNER JOIN ANALYTICS.ANALYTICS_STAGING.dim_products ON dim_products.PRODUCT_ID=fct_charges.PRODUCT_ID
    INNER JOIN ANALYTICS.ANALYTICS_STAGING.dim_subscriptions ON dim_subscriptions.SUBSCRIPTION_ID = fct_charges.subscription_id
    INNER JOIN  ANALYTICS.ANALYTICS_STAGING.dim_customers ON dim_customers.crm_id = dim_subscriptions.crm_id
;

```

## Useful links 
* Discourse about Kimball dimensional modelling in modern data warehouses includding some important ideas why we should still sue Kimball
* https://discourse.getdbt.com/t/is-kimball-dimensional-modeling-still-relevant-in-a-modern-data-warehouse/225/6