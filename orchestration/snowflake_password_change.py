#!/usr/bin/env python3
import logging
import sys
from os import environ as env
from typing import Dict, List

from fire import Fire
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)


class SnowflakeManager:
    def __init__(self, config_vars: Dict):
        self.engine = create_engine(
            URL(
                user=config_vars["PERMISSION_BOT_USER"],
                password=config_vars["PERMISSION_BOT_PASSWORD"],
                account=config_vars["PERMISSION_BOT_ACCOUNT"],
                role=config_vars["PERMISSION_BOT_ROLE"],
                database=config_vars["PERMISSION_BOT_DATABASE"],
                warehouse=config_vars["PERMISSION_BOT_WAREHOUSE"],
            )
        )

    def show_users(self) -> pd.DataFrame:
        query = "SHOW USERS;"
        connection = self.engine.connect()
        results = pd.read_sql(sql=query, con=connection)
        self.engine.dispose()
        return results

    def set_password_reset(self) -> None:
        user_list = self.show_users()
        exemption_list = [
            "AIRFLOW",
            "FIVETRAN",
            "GITLAB_CI",
            "PERISCOPE",
            "PERISCOPE_SENSITIVE",
            "PERISCOPE_STAGING",
            "PERMISSION_BOT",
            "SNOWFLAKE",
            "TARGET_SNOWFLAKE",
        ]
        connection = self.engine.connect()
        for index, row in user_list.iterrows():
            user_name = row["name"]
            if user_name not in exemption_list:
                query = f"ALTER USER {user_name} SET MUST_CHANGE_PASSWORD = TRUE;"
            else:
                continue
            logging.info(f"Executing {query}")
            connection.execute(query)

        self.engine.dispose()


if __name__ == "__main__":
    snowflake_manager = SnowflakeManager(env.copy())
    Fire(snowflake_manager)
