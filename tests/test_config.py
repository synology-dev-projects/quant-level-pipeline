import src.config as config

def test_find_project_root(env_config):
    """
    Verifies that the .env file exists and that Pydantic reads it correctly.
    """
    assert str(env_config.target_env_path).endswith(r"quant-level-pipeline\.env"), \
        f"Expected path to end with 'quant-level-pipeline\.env' but got: {config.target_env_path}"


def test_load_config_from_env(env_config):
    """
    Verifies that the .env file exists and that Pydantic reads it correctly.
    """
    print(env_config.model_dump_json(indent=2))
    assert env_config.oracle_user != ""
    assert env_config.oracle_pass != ""
    assert env_config.oracle_host_ip != ""
    assert env_config.oracle_service != ""