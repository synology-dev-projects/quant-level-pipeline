# tests/conftest.py
import sys
from pathlib import Path
import pytest

src_path = str(Path(__file__).resolve().parent.parent / "src")
common_lib_path = str(Path(__file__).resolve().parent.parent.parent / "common-lib")
if src_path not in sys.path:
    sys.path.insert(0, src_path)
if common_lib_path not in sys.path:
    sys.path.insert(0, common_lib_path)

import extract, transform
import common_lib.config.main_config as config



@pytest.fixture(scope="session")
def env_config():
    """Load config once for the whole session."""
    return config.load_config()


@pytest.fixture(scope="session")
def pipeline_data(env_config):
    """
    Runs the expensive pipeline ONCE and returns a dictionary
    containing all intermediate dataframes/variables.
    """
    print("\n[Setup] Running expensive pipeline extraction...")

    raw_post_json = extract.run(env_config, cutoff_date=None)

    # 2. Transform unstructured data to structured df
    clean_df = transform.run(env_config, raw_post_json)


    # 2. Return EVERYTHING in a dictionary
    return {
        "raw_post_json": raw_post_json,
        "clean_df": clean_df,
    }