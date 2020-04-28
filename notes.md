# Dimensional modelling


## Fact and dimension tables

The high level schema of fact and dimension tables for calculating  ARR/ Customer count
```mermaid
classDiagram
	fct_charges --|> dim_subscriptions
	fct_charges --|> dim_accounts
        fct_charges --|> dim_products
        fct_charges --|> dim_dates
        dim_accounts --|> dim_customers
        dim_subscriptions --|> dim_customers
        fct_invoice_items_agg <|-- fct_charges
	fct_charges : FK: invoice_item_id
	fct_charges : FK: account_id
	fct_charges: FK: product_id
        fct_charges: FK: subscription_id,
        fct_charges: FK: effective_start_date_id, 
        fct_charges: FK: effective_end_date_id, 
        fct_charges : PK: charge_id
	fct_charges: mrr()
        fct_charges: rate_plan_name()
        fct_charges:  rate_plan_charge_name()
        fct_invoice_items_agg: FK: charge_id
        fct_invoice_items_agg: charge_amount_sum()
	class dim_accounts{
		PK: account_id

		account_name()
		country()
	}
        class dim_accounts{
		PK: account_id
                FK: crm_id
		account_name()
		country()
	}
	class dim_products{
		PK: product_id
		product_category()
	}
	class dim_subscriptions{
		PK: subscription_id
                FK: crm_id
		subscription_status()
	}
        class dim_dates {
                PK: date_id
        }
        class dim_customers {
                PK: crm_id
                customer_id
        }

```		



## How to interact with the dim/fct tables -  building Data Marts

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