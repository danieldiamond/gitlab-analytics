import pandas as pd
import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


zuora_missing_crm_id = """
with zuora_mrr_totals as (

    SELECT * FROM analytics_staging.zuora_mrr_totals

), zuora_account as (

    SELECT * FROM analytics_staging.zuora_account

), sfdc_accounts_xf as (

    SELECT * FROM analytics.sfdc_accounts_xf

), sfdc_deleted_accounts as (

    SELECT * FROM analytics_staging.sfdc_deleted_accounts

), joined as (

    SELECT zuora_account.account_id     as zuora_account_id,
           zuora_account.account_name,
           zuora_account.crm_id,
           sfdc_accounts_xf.account_id  as sfdc_account_id,
           sfdc_accounts_xf.ultimate_parent_account_id
    FROM zuora_mrr_totals
    LEFT JOIN zuora_account
        ON zuora_account.account_number = zuora_mrr_totals.account_number
    LEFT JOIN sfdc_accounts_xf
        ON sfdc_accounts_xf.account_id = zuora_account.crm_id

), final as (

    SELECT zuora_account_id,
            account_name,
            crm_id,
            coalesce(joined.sfdc_account_id, sfdc_master_record_id) as sfdc_account_id
    FROM joined
    LEFT JOIN sfdc_deleted_accounts
    ON crm_id = sfdc_deleted_accounts.sfdc_account_id
)

SELECT *
FROM final
WHERE sfdc_account_id IS NULL
GROUP BY 1, 2, 3, 4

"""


def main(query_name):
    snowflake_url = URL(
        account="gitlab",
        user=os.environ["SNOWFLAKE_TRANSFORM_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="ANALYTICS",
        warehouse="ANALYST_XS",
        role="TRANSFORMER",  ## needs to be a role that can see sensitive data
    )
    engine = create_engine(snowflake_url)
    connection = engine.connect()
    df1 = pd.read_sql(sql=query_name, con=connection)
    print(df1)


## main
if __name__ == "__main__":
    ## start snowflake engine
    print("starting")
    ## execute query that produces failure result
    main(query_name=zuora_missing_crm_id)
    print("done")
    ## enhance result, e.g. zuora url
