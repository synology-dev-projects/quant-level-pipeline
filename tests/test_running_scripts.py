import pytest

import  load, extract, transform
import pandas as pd

import common_lib.config.main_config as config
import common_lib.connectors.postgres as postgres
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

    pg_df = postgres.sql(env_config, "SELECT * FROM quant_lvl_data_te")

    # Check 1: Data extraction check (ensure historical data is non-empty)
    assert not clean_df.empty, "Extracted DataFrame should not be empty"
    assert clean_df['DATETIME'].nunique() > 100, "Should have a reasonable baseline of historical trading days"

    # Check 2: 1-to-1 Sync Check (Postgres DB must match extracted DataFrame exactly)
    assert pg_df['DATETIME'].nunique() == clean_df['DATETIME'].nunique(), "Postgres unique date count must match clean_df"
    assert len(pg_df) == len(clean_df), "Postgres row count must match clean_df row count"

@pytest.mark.integration
def test_incremental_load(env_config, pipeline_data, monkeypatch):
    """
    Verifies that incremental load works correctly.
    """
    # Mock ntfy push notification to prevent spamming live alerts
    mock_resp = type("MockResponse", (), {"status_code": 200})()
    monkeypatch.setattr(nfty, "send_ntfy_notification", lambda *args, **kwargs: mock_resp)

    # delete all records of highest_date
    postgres.execute(env_config,
               """
               DELETE FROM quant_lvl_data_te
                    WHERE DATETIME = (
                        SELECT MAX(DATETIME) 
                        FROM quant_lvl_data_te
                    )
               """
               )

    # get count
    count_before_load = postgres.sql(env_config, "SELECT count(1) FROM quant_lvl_data_te").iloc[0, 0]

    # 1. Run entire pipeline
    cuffoff_date = load._get_latest_recorded_date(env_config)
    raw_post_json = extract.run(env_config, cutoff_date=cuffoff_date)
    clean_df = transform.run(env_config, raw_post_json)
    load.run(env_config, "upsert", clean_df)

    market_now = pd.Timestamp.now(tz='US/Eastern').date()

    count_after_load = postgres.sql(env_config, "SELECT count(1) FROM quant_lvl_data_te").iloc[0, 0]

    df_str = load._quant_lvl_df_to_string(clean_df)
    nfty_response = nfty.send_ntfy_notification(env_config.ntfy_endpoint, "quant_alerts", "TEST_QUANT_MESSAGE", df_str, 3)

    # Check 1: check recall of all days for quant lvls
    assert clean_df['DATETIME'].nunique() >= 1

    # Check 2: Smoke check to see if all records got through
    latest_dt = clean_df['DATETIME'].max()
    new_records_count = len(clean_df[clean_df['DATETIME'] == latest_dt])
    assert (count_after_load - count_before_load) == new_records_count

    # Check 3: Smoke check to see if all records got through
    assert nfty_response.status_code == 200



