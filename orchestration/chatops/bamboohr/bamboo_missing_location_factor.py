import pandas as pd
import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


no_missing_location_factors = """
WITH source as (

  SELECT *
  FROM "ANALYTICS".sensitive.employee_directory

)

SELECT employee_number, left(first_name, 1) || '. ' || left(last_name, 1) || '.' as initials
FROM source
WHERE hire_location_factor IS NULL
AND termination_date IS NULL
AND CURRENT_DATE > dateadd('days', 7, hire_date)
"""


def main(query_name):
    snowflake_url = URL(
        account="gitlab",
        user=os.environ["SNOWFLAKE_TRANSFORM_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="ANALYTICS",
        warehouse="ANALYST_XS",
        role="TRANSFORMER",
    )
    # authenticator='https://gitlab.okta.com',
    engine = create_engine(snowflake_url)
    connection = engine.connect()
    df1 = pd.read_sql(sql=query_name, con=connection)
    print(df1)


## main
if __name__ == "__main__":
    ## start snowflake engine
    print("starting")
    ## execute query that produces failure result
    main(query_name=no_missing_location_factors)
    print("done")
    ## enhance result, e.g. zuora url
