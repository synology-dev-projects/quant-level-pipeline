import extract, transform, load
import logging
logger = logging.getLogger(__name__)
import sys
import common_lib.config.main_config as config
import common_lib.connectors.nfty as nfty


def main():
    env_config = config.load_config()

    try:
        cutoff_date = load._get_latest_recorded_date(env_config)

        # 1. Fetch raw data from site (cutoff_date=None)
        raw_post_json = extract.run(env_config, cutoff_date=cutoff_date)

        if not raw_post_json:
            logging.info(f"No new posts found after cutoff_date: {cutoff_date}. Exiting cleanly.")
            sys.exit(0)

        # 2. Transform unstructured data to structured df
        clean_df = transform.run(env_config, raw_post_json)

        df_str = load._quant_lvl_df_to_string(clean_df)
        nfty.send_ntfy_notification(
            env_config.ntfy_endpoint,
            "quant_alerts",
            "NEW QUANT LVLS",
            df_str,
            3
        )

        # 3. Load df to postgres
        load.run(env_config, "upsert", clean_df)
        logging.info("Daily incremental quant levels loaded successfully.")

    except SystemExit:
        raise
    except Exception as e:
        error_msg = f"CRITICAL: daily_incremental.py failed with exception: {e}"
        logging.exception(error_msg)
        try:
            nfty.send_ntfy_notification(
                env_config.ntfy_endpoint,
                "quant_alerts",
                "🚨 PIPELINE FAILURE: Quant Levels",
                error_msg,
                5
            )
        except Exception as alert_err:
            logging.error(f"Failed to dispatch error notification: {alert_err}")
        sys.exit(1)


if __name__ == "__main__":
    main()