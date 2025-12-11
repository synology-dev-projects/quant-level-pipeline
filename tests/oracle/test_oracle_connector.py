import pytest

import pandas as pd
import unittest

import src.connectors.oracle as oracle_conn
from src.connectors.config import config

@pytest.fixture
def oracle():
    return oracle_conn.OracleConnector(config)



class TestConnector():

    def test_config_load(self):
        """
        Ensures all configs needed for this pipeline are loadable
        """
        print(config.model_dump_json(indent=2))
        assert config.oracle_user != ""
        assert config.oracle_pass != ""
        assert config.oracle_host_ip != ""
        assert config.oracle_service != ""

    def test_write_to_oracle_upsert(self, oracle):
        """
        Checks if no constraints exceptions raised when inserting
        Passes if new records are found
        """

        df1 = pd.DataFrame({
            'symbol': ['SPY', 'SPY', 'SPY'],
            'datetime': pd.to_datetime(['2023-01-15', '2023-01-16', '2023-01-17']),
            'price': [12.50, 12.75, 13.00]
        })
        df2 = pd.DataFrame({
            'symbol': ['SPY', 'SPY', 'SPY'],
            'datetime': pd.to_datetime(['2023-01-15', '2023-01-16', '2023-01-17']),
            'price': [1.00, 2.00, 3.00]
        })

        temp_table = oracle._df_to_oracle_overwrite(df1, "ticker_test", ["symbol", "datetime"])
        oracle._df_to_oracle_upsert(df2, "ticker_test", ["symbol", "datetime"])

        returned_df = oracle.sql("SELECT * FROM ticker_test")
        oracle.drop_table_if_exists("ticker_test")

        print(returned_df)

        assert (pd.DataFrame.equals(df1, returned_df)) is False


    def test_write_to_oracle_insert_ignore(self, oracle):
        """
        Checks if records are being ignored when inserting
        Passes if no exception raised and df remains the same
        """
        df1 = pd.DataFrame({
            'SYMBOL': ['SPY', 'SPY', 'SPY'],
            'DATETIME': pd.to_datetime(['2023-01-15', '2023-01-16', '2023-01-17']),
            'PRICE': [12.50, 12.75, 13.00]
        })

        df2 = pd.DataFrame({
            'SYMBOL': ['SPY', 'SPY', 'SPY', 'SPY'],
            'DATETIME': pd.to_datetime(['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18']),
            'PRICE': [1.00, 2.00, 3.00, 4.00]
        })

        temp_table = oracle._df_to_oracle_overwrite(df1, "ticker_test", ["SYMBOL", "DATETIME"])
        oracle._df_to_oracle_insert_ignore(df2, "ticker_test", ["SYMBOL", "DATETIME"])

        returned_df = oracle.sql("SELECT * FROM ticker_test")
        oracle.drop_table_if_exists("ticker_test")


        print(returned_df)

        merged_df = returned_df.merge(df1, on=['SYMBOL', 'DATETIME', 'PRICE'], how='left', indicator=True)

        assert len(returned_df) == 4
        assert (pd.DataFrame.equals(df1, merged_df[merged_df['_merge'] == 'both'].drop('_merge', axis=1)))



if __name__ == '__main__':
    unittest.main()