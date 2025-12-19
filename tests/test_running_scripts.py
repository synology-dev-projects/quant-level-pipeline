import pytest

import config, load, extract, transform
import pandas as pd

from connectors import oracle




def test_historical_load(env_config, pipeline_data):
    """
    Verifies that the .env file exists and that Pydantic reads it correctly.
    """

    # 1. Run entire pipeline
    raw_post_json = extract.run(env_config, cutoff_date=None)
    clean_df = transform.run(env_config,raw_post_json)
    load.run(env_config, "overwrite", clean_df)

    num_of_business_days_since_cutoff = len(pd.bdate_range('2025-06-17', pd.Timestamp.now().date())) - 5

    oracle_df = oracle.sql(env_config, f"SELECT * FROM {env_config.oracle_quant_table_name}")

    # Check 1: check recall of all days for quant lvls
    assert clean_df['DATETIME'].nunique() >= num_of_business_days_since_cutoff
    assert oracle_df['DATETIME'].nunique() >= num_of_business_days_since_cutoff

    # Check 2: Smoke check to see if all records got through
    assert len(oracle_df) == len(clean_df)

def test_incremental_load(env_config, pipeline_data):
    """
    Verifies that the .env file exists and that Pydantic reads it correctly.
    """

    #delete all records of highest_date
    oracle.execute(env_config,
               f"""
               DELETE FROM {env_config.oracle_quant_table_name}
                    WHERE DATETIME = (
                        SELECT MAX(DATETIME) 
                        FROM {env_config.oracle_quant_table_name}
                    )
               """
               )

    #get count
    count_before_load = oracle.sql(env_config, f"SELECT count(1) FROM {env_config.oracle_quant_table_name}").iloc[0, 0]

    # 1. Run entire pipeline
    cuffoff_date = load._get_latest_recorded_date(env_config)
    raw_post_json = extract.run(env_config, cutoff_date=cuffoff_date)
    clean_df = transform.run(env_config,raw_post_json)
    load.run(env_config, "upsert", clean_df)

    num_of_business_days_since_cutoff = len(pd.bdate_range(cuffoff_date.strftime('%Y-%m-%d'), pd.Timestamp.now().date())) - 1

    count_after_load = oracle.sql(env_config, f"SELECT count(1) FROM {env_config.oracle_quant_table_name}").iloc[0, 0]
    oracle_df = oracle.sql(env_config, f"SELECT * FROM {env_config.oracle_quant_table_name}")


    # Check 1: check recall of all days for quant lvls
    assert clean_df['DATETIME'].nunique() >= num_of_business_days_since_cutoff

    # Check 2: Smoke check to see if all records got through
    assert (count_after_load - count_before_load) == len(clean_df)


