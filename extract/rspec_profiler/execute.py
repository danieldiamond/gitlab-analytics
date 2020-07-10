import pandas as pd
from os import environ as env

from gitlabdata.orchestration_utils import (

    snowflake_engine_factory,
    dataframe_uploader,
)


config_dict = env.copy()

if __name__ == "__main__":

    df = pd.read_csv("overall_time.csv")

    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    dataframe_uploader(dataframe=df,
                       engine=snowflake_engine,
                       table_name="profiling_data",
                       schema="rspec",
                       )
