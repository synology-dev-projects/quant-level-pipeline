from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr, model_validator
from pathlib import Path
import logging
import sys

# 1. Calculate the Dynamic Path
# Start: git-repos/env/project_root/src/connectors/config.py -> End: git-repos/conf/.env
current_file_path = Path(__file__).resolve()
project_root_path = current_file_path.parent.parent.parent
target_env_path = project_root_path / ".env"



class Config(BaseSettings):
    oracle_user: str = Field(...)
    oracle_pass: SecretStr = Field(...)
    oracle_host_ip: str = Field(...)
    oracle_service: str = Field(...)

    # Automatically look for a .env file
    model_config = SettingsConfigDict(
        env_file=str(target_env_path),
        env_file_encoding="utf-8",
        extra="ignore"
    )




# Singleton instance
try:
    config = Config()
except Exception as e:
    logging.error(f"CRITICAL CONFIG ERROR: {e}")
    sys.exit(1)