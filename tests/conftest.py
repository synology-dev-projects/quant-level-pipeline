# tests/conftest.py
import pytest
import extract, transform, config


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