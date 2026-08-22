import pytest

import  load, extract, transform
import pandas as pd

import common_lib.config.main_config as config
import common_lib.connectors.oracle as oracle
import common_lib.connectors.nfty as nfty



@pytest.mark.integration
def test_historical_load(env_config, pipeline_data):
    """
    Integration test: Verifies historical load pipeline against test database.
    """
    # 1. Run entire pipeline
    raw_post_json = extract.run(env_config, cutoff_date=None)
    clean_df = transform.run(env_config, raw_post_json)
    load.run(env_config, "overwrite", clean_df)

    oracle_df = oracle.sql(env_config, f"SELECT * FROM {env_config.oracle_quant_table_name}")

    # Check 1: Data extraction check (ensure historical data is non-empty)
    assert not clean_df.empty, "Extracted DataFrame should not be empty"
    assert clean_df['DATETIME'].nunique() > 100, "Should have a reasonable baseline of historical trading days"

    # Check 2: 1-to-1 Sync Check (Oracle DB must match extracted DataFrame exactly)
    assert oracle_df['DATETIME'].nunique() == clean_df['DATETIME'].nunique(), "Oracle unique date count must match clean_df"
    assert len(oracle_df) == len(clean_df), "Oracle row count must match clean_df row count"

@pytest.mark.integration
def test_incremental_load(env_config, pipeline_data, monkeypatch):
    """
    Verifies that incremental load works correctly.
    """
    # Mock ntfy push notification to prevent spamming live alerts
    mock_resp = type("MockResponse", (), {"status_code": 200})()
    monkeypatch.setattr(nfty, "send_ntfy_notification", lambda *args, **kwargs: mock_resp)

    # delete all records of highest_date
    oracle.execute(env_config,
               f"""
               DELETE FROM {env_config.oracle_quant_table_name}
                    WHERE DATETIME = (
                        SELECT MAX(DATETIME) 
                        FROM {env_config.oracle_quant_table_name}
                    )
               """
               )

    # get count
    count_before_load = oracle.sql(env_config, f"SELECT count(1) FROM {env_config.oracle_quant_table_name}").iloc[0, 0]

    # 1. Run entire pipeline
    cuffoff_date = load._get_latest_recorded_date(env_config)
    raw_post_json = extract.run(env_config, cutoff_date=cuffoff_date)
    clean_df = transform.run(env_config, raw_post_json)
    load.run(env_config, "upsert", clean_df)

    market_now = pd.Timestamp.now(tz='US/Eastern').date()

    count_after_load = oracle.sql(env_config, f"SELECT count(1) FROM {env_config.oracle_quant_table_name}").iloc[0, 0]

    df_str = load._quant_lvl_df_to_string(clean_df)
    nfty_response = nfty.send_ntfy_notification(env_config.ntfy_endpoint, "quant_alerts", "TEST_QUANT_MESSAGE", df_str, 3)

    # Check 1: check recall of all days for quant lvls
    assert clean_df['DATETIME'].nunique() >= 1

    # Check 2: Smoke check to see if all records got through
    assert (count_after_load - count_before_load) == len(clean_df)

    # Check 3: Smoke check to see if all records got through
    assert nfty_response.status_code == 200


