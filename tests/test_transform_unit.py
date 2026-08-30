import pytest
import pandas as pd
from unittest.mock import MagicMock
from transform import (
    _parse_quant_levels_to_data,
    _deduplicate_days,
    _deduplicate_rows,
    _clean_df,
    _define_quant_dataframe,
    run
)


def create_mock_config():
    mock_config = MagicMock()
    mock_config.oracle_quant_pks = ["DATETIME", "TICKER", "START_LVL_PRICE"]
    return mock_config


def test_parse_quant_levels_to_data_with_zones():
    posts = [
        {
            "id": "1",
            "date_posted": "2026-08-20T10:00:00Z",
            "title": "Quant Levels Aug 20",
            "link": "https://tradingedge.club/posts/1",
            "quant_lvl_text": """
6050 - 6060 resistance
6000 pivot
---
5950 - 5960 buy zone
5900 buy
---
6100 sell zone
"""
        }
    ]
    df = _parse_quant_levels_to_data(posts)
    assert not df.empty
    assert "DATETIME" in df.columns
    assert "TICKER" in df.columns
    assert "START_LVL_PRICE" in df.columns
    assert "BUY_SELL_IND" in df.columns

    # Verify BUY section parsed
    buy_rows = df[df["BUY_SELL_IND"] == "BUY"]
    assert len(buy_rows) >= 1

    # Verify SELL section parsed
    sell_rows = df[df["BUY_SELL_IND"] == "SELL"]
    assert len(sell_rows) >= 1


def test_deduplicate_days():
    df = pd.DataFrame([
        {
            "DATETIME": pd.Timestamp("2026-08-20 10:00:00+00:00"),
            "TICKER": "SPX",
            "START_LVL_PRICE": 6000.0,
            "END_LVL_PRICE": None,
            "COMMENTS": "test",
            "BUY_SELL_IND": None,
            "WEB_LINK": "https://example.com/1"
        },
        {
            "DATETIME": pd.Timestamp("2026-08-20 14:00:00+00:00"),
            "TICKER": "SPX",
            "START_LVL_PRICE": 6010.0,
            "END_LVL_PRICE": None,
            "COMMENTS": "updated later",
            "BUY_SELL_IND": None,
            "WEB_LINK": "https://example.com/2"
        }
    ])
    deduped = _deduplicate_days(df)
    assert len(deduped) == 1
    assert deduped.iloc[0]["WEB_LINK"] == "https://example.com/2"


def test_deduplicate_rows():
    mock_config = create_mock_config()
    df = pd.DataFrame([
        {
            "DATETIME": pd.Timestamp("2026-08-20 10:00:00"),
            "TICKER": "SPX",
            "START_LVL_PRICE": 6000.0,
            "END_LVL_PRICE": None,
            "COMMENTS": "dup1",
            "BUY_SELL_IND": None,
            "WEB_LINK": "https://example.com/1"
        },
        {
            "DATETIME": pd.Timestamp("2026-08-20 10:00:00"),
            "TICKER": "SPX",
            "START_LVL_PRICE": 6000.0,
            "END_LVL_PRICE": 6005.0,
            "COMMENTS": "dup2",
            "BUY_SELL_IND": "BUY",
            "WEB_LINK": "https://example.com/1"
        }
    ])
    result = _deduplicate_rows(mock_config, df)
    assert len(result) == 1


def test_clean_df():
    mock_config = create_mock_config()
    df = pd.DataFrame([
        {
            "DATETIME": pd.Timestamp("2026-08-20 10:00:00"),
            "TICKER": "SPX",
            "START_LVL_PRICE": 6000.0,
            "END_LVL_PRICE": None,
            "COMMENTS": "test",
            "BUY_SELL_IND": None,
            "WEB_LINK": "https://example.com/1"
        }
    ])
    cleaned = _clean_df(mock_config, df)
    assert cleaned["DATETIME"].iloc[0] == pd.Timestamp("2026-08-20")


def test_transform_run_e2e_mocked():
    mock_config = create_mock_config()
    mock_posts = [
        {
            "id": "p1",
            "date_posted": "2026-08-25T08:00:00Z",
            "title": "SPX Quant Levels",
            "link": "https://tradingedge.club/posts/p1",
            "quant_lvl_text": """
6000 - 6010 resistance
5950 pivot
---
5900 buy zone
---
6050 sell zone
"""
        }
    ]
    df = run(mock_config, mock_posts)
    assert not df.empty
    assert len(df) >= 3
    assert set(df["TICKER"].unique()) == {"SPX"}
