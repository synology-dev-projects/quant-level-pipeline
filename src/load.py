import logging
import pandas as pd
from datetime import datetime, timezone
from src.connectors import oracle
from src.config import Config
import sys

class CutoffDateNotFoundError(Exception):
    """Raised when the database query returns no valid max date."""
    pass

logger = logging.getLogger(__name__)


def run(config: Config, write_mode: str, df: pd.DataFrame) -> None:
    table_name = config.oracle_quant_table_name
    primary_keys = config.oracle_quant_pks

    if df.empty:
        logging.error("DataFrame is empty. Skipping DB push.")
        sys.exit(1)

    logging.info(f"Pushing {len(df)} rows to '{table_name}' with mode='{write_mode}'...")

    try:
        oracle.insert_into_table(
            config=config,
            df=df,
            table_name=table_name,
            write_mode=write_mode,
            primary_keys=primary_keys
        )

        logging.info("Push successful.")

    except Exception as e:
        logging.error(f"Failed to push to Oracle: {e}")
        raise e




def _get_latest_recorded_date(config: Config) -> datetime:
    """
    Gets the latest record records date of the quant_lvl_table  for cuttoff date
    :param config:
    :return:
    """
    query = f'SELECT MAX(DATETIME) FROM {config.oracle_quant_table_name}'

    try:
        # 1. Run Query
        df = oracle.sql(config, query)

        # 2. Check for Empty DataFrame (Rare for Aggregations)
        if df.empty:
            raise CutoffDateNotFoundError("Query returned no rows.")

        # 3. Check for Null/NaT (Common: Table exists but has 0 rows)
        last_date = df.iloc[0, 0]
        if pd.isna(last_date):
            raise CutoffDateNotFoundError(f"Table '{config.oracle_quant_table_name} is empty; no max date found.")

        # 4. Success Case: Process the date
        if isinstance(last_date, pd.Timestamp):
            last_date = last_date.to_pydatetime()

        if last_date.tzinfo is None:
            last_date = last_date.replace(tzinfo=timezone.utc)

        logger.info(f"Last checkpoint found: {last_date}")
        return last_date

    except CutoffDateNotFoundError:
        # Re-raise the custom error so the caller sees it
        raise

    except Exception as e:
        # Wrap generic database errors (like "Table not found") into your custom error
        logger.warning(f"Database error while fetching date: {e}")
        raise CutoffDateNotFoundError(f"Could not retrieve cutoff date due to DB error: {e}")


