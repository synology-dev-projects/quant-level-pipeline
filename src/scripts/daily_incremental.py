import extract, transform, load, config


def main():
    env_config = config.load_config()

    cuffoff_date = load._get_latest_recorded_date(env_config)

    # 1. Fetch raw data from site (cutoff_date=None)
    raw_post_json = extract.run(env_config, cutoff_date=cuffoff_date)

    # 2. Transform unstructured data to structured df
    clean_df = transform.run(env_config,raw_post_json)

    # 3. Load df to oracle
    load.run(env_config, "upsert", clean_df)


if __name__ == "__main__":
    main()