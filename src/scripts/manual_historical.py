import extract, transform, load, config
import logging
logger = logging.getLogger(__name__)
import sys

def main():
    env_config = config.load_config()

    # 1. Fetch raw data from site (cutoff_date=None)
    raw_post_json = extract.run(env_config, cutoff_date=None)

    if len(raw_post_json) == 0:
        logging.error(f"ERROR: No post found for historical load. Please check if website it up")
        sys.exit(1)

    # 2. Transform unstructured data to structured df
    clean_df = transform.run(env_config, raw_post_json)

    # 3. Load df to oracle
    load.run(env_config, "overwrite",clean_df)



if __name__ == "__main__":
    main()