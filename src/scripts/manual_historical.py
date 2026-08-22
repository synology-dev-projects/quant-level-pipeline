import extract, transform, load
import logging
logger = logging.getLogger(__name__)
import sys
import common_lib.config.main_config as config
import common_lib.connectors.nfty as nfty

def main():
    env_config = config.load_config()

    try:
        # 1. Fetch raw data from site (cutoff_date=None)
        raw_post_json = extract.run(env_config, cutoff_date=None)

        if not raw_post_json:
            logging.error(f"ERROR: No posts found for historical load. Please check if website is accessible.")
            sys.exit(1)

        # 2. Transform unstructured data to structured df
        clean_df = transform.run(env_config, raw_post_json)

        # 3. Load df to oracle
        load.run(env_config, "overwrite", clean_df)
        logging.info("Historical quant levels loaded successfully.")

    except SystemExit:
        raise
    except Exception as e:
        error_msg = f"CRITICAL: manual_historical.py failed with exception: {e}"
        logging.exception(error_msg)
        try:
            nfty.send_ntfy_notification(
                env_config.ntfy_endpoint,
                "quant_alerts",
                "🚨 PIPELINE FAILURE: Historical Quant Load",
                error_msg,
                5
            )
        except Exception as alert_err:
            logging.error(f"Failed to dispatch error notification: {alert_err}")
        sys.exit(1)


if __name__ == "__main__":
    main()