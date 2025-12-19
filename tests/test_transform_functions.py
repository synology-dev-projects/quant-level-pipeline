from datetime import datetime, timezone, date

import pandas as pd

from transform import _define_quant_dataframe

pd.set_option('display.max_rows', None)      # Show all rows
pd.set_option('display.max_columns', None)   # Show all columns
pd.set_option('display.width', 1000)         # Auto-detect width to prevent wrapping
pd.set_option('display.max_colwidth', 100)  # Don't truncate long text in cells


#--------------------------------------SPECIFIC CASES-------------------------------------------------------#

def test_parse_quant_lvl_post(env_config, pipeline_data):
    """
    if input template found in post
    :return:
    """
    web_link = "https://tradingedge.club/posts/89336017"
    df = pipeline_data["clean_df"]

    expected_date = pd.Timestamp("2025-08-18")
    quant_lvls_for_posts = df[df['DATETIME'] == expected_date].sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)

    expected_data = [
        [expected_date, "SPX", 6497.0, 6500.0, "high likelihood of resistance", None, web_link],
        [expected_date, "SPX", 6485.0, None, None, None, web_link],
        [expected_date, "SPX", 6463.0, None, None, None, web_link],
        [expected_date, "SPX", 6455.0, None, "pivot", None, web_link],
        [expected_date, "SPX", 6440.0, None, None, None, web_link],
        [expected_date, "SPX", 6414.0, None, None, None, web_link],
      #  [expected_date, "SPX", 6400.0, 6403.0, None, None, web_link],
        [expected_date, "SPX", 6395.0, None, None, None, web_link],
        [expected_date, "SPX", 6383.0, None, None, None, web_link],
        [expected_date, "SPX", 6400.0, 6403.0, "high likelihood of support", "BUY", web_link],
        [expected_date, "SPX", 6480.0, 6500.0, None, "SELL", web_link]
    ]

    # 1. Check what is actually in the column
    print("Type:", df['DATETIME'].dtype)
    print("First row:", df['DATETIME'].iloc[0])

    # 2. Check your target date
    print("Target:", date(2025, 8, 18))

    print(sorted(df['DATETIME'].dt.date.unique()))

    expected_df = _define_quant_dataframe(expected_data).sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)



    try:
        pd.testing.assert_frame_equal(expected_df, quant_lvls_for_posts)
    except AssertionError as e:
        print("Missing from expected:\n", except_distinct(expected_df, quant_lvls_for_posts))
        print("Excess in target:\n", except_distinct(quant_lvls_for_posts, expected_df))
        raise e


def test_parse_quant_lvl_post_2(env_config, pipeline_data):
    """
     if no input template found
    :return:
    """
    web_link = "https://tradingedge.club/posts/86408272"
    df = pipeline_data["clean_df"]
    expected_date = pd.Timestamp("2025-06-20")
    quant_lvls_for_posts = df[df['DATETIME'] == expected_date].sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)

    expected_data = [
        [expected_date, "SPX", 6062.0, None, None, None, web_link],
        [expected_date, "SPX", 6050.0, None, None, None, web_link],
        [expected_date, "SPX", 6034.0, None,
         "if the high probability reversal point hits, then look here for a reversal.", None, web_link],
        [expected_date, "SPX", 6010.0, 6017.0, "high probability reversal point.", None, web_link],
        [expected_date, "SPX", 5995.0, 6000.0, "lots of gamma here - pinning potential.", None, web_link],
        [expected_date, "SPX", 5958.0, 5970.0, "pivot", None, web_link],
        [expected_date, "SPX", 5950.0, 5955.0, "21d ema and also the gamma flip", None, web_link],
        [expected_date, "SPX", 5925.0, None, None, None, web_link],
        [expected_date, "SPX", 5913.0, None, None, None, web_link],
        [expected_date, "SPX", 5895.0, 5905.0, "near the JPM , strong support", None, web_link],
        [expected_date, "SPX", 5850.0, None, "if the 5895 level breaks, watch here.", None, web_link]
    ]

    expected_df = _define_quant_dataframe(expected_data).sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(expected_df, quant_lvls_for_posts)
    except AssertionError as e:
        print("Missing from expected:\n", except_distinct(expected_df, quant_lvls_for_posts))
        print("Excess in target:\n", except_distinct(quant_lvls_for_posts, expected_df))
        raise e


def test_parse_quant_lvl_post_3(env_config, pipeline_data):
    """
    This particular test shows how there may be overlapping levels.
    Checks whether the code is smart enough to combine these levels so they are unique
    :return:
    """
    web_link = "https://tradingedge.club/posts/89883132"
    pks = ["DATETIME", "TICKER", "START_LVL_PRICE"]

    df = pipeline_data["clean_df"]

    expected_date = pd.Timestamp("2025-08-28")
    quant_lvls_for_posts = (df[df['DATETIME'] == expected_date]
                            .sort_values(by=["BUY_SELL_IND"] + pks,
                                         na_position='first',
                                         ascending=[True, False, False, False])
                            .reset_index(drop=True))

    expected_data = [
        [expected_date, "SPX", 6548.0, None, None, None, web_link],
        [expected_date, "SPX", 6528.0, None, "high likelihood of reversal", None, web_link],
        # [expected_date, "SPX", 6520.0, None, None, None, web_link], we remove as its kinda duplicated
        [expected_date, "SPX", 6500.0, None, None, None, web_link],
        # [expected_date, "SPX", 6486.0, 6493.0, None, None, web_link], we remove as its kinda duplicated
        [expected_date, "SPX", 6462.0, None, None, None, web_link],
        [expected_date, "SPX", 6445.0, 6449.0, "9d EMA", None, web_link],
        # [expected_date, "SPX", 6433.0, None, None, None, web_link], we remove as its kinda duplicated
        [expected_date, "SPX", 6410.0, None, "21d EMA", None, web_link],
        [expected_date, "SPX", 6392.0, 6397.0, None, None, web_link],
        [expected_date, "SPX", 6433.0, 6449.0, None, "BUY", web_link],
        [expected_date, "SPX", 6486.0, 6493.0, "first, minor resistance", "SELL", web_link],
        [expected_date, "SPX", 6520.0, 6528.0, "main resistance", "SELL", web_link]
    ]

    expected_df = _define_quant_dataframe(expected_data)

    expected_df.sort_values(by=["BUY_SELL_IND"] + pks, na_position='first', ascending=[True, False, False, False],
                            inplace=True)
    expected_df.reset_index(drop=True, inplace=True)
    df.sort_values(by=["BUY_SELL_IND"] + pks, na_position='first', ascending=[True, False, False, False], inplace=True)
    df.reset_index(drop=True, inplace=True)

    duplicates = df.duplicated(subset=pks, keep=False)
    check_for_duplicates = duplicates.all()

    assert not check_for_duplicates

    try:
        pd.testing.assert_frame_equal(expected_df, quant_lvls_for_posts)
    except AssertionError as e:
        print("Missing from expected:\n", except_distinct(expected_df, quant_lvls_for_posts))
        print("Excess in target:\n", except_distinct(quant_lvls_for_posts, expected_df))
        raise e


def test_parse_quant_lvl_file_5(env_config, pipeline_data):
    """
    Check to see if it parses the quant_file properly
    :return:
    """
    web_link = "https://tradingedge.club/posts/90939812"

    df = pipeline_data["clean_df"]
    expected_date = pd.Timestamp("2025-09-18")
    quant_lvls_for_posts = df[df['DATETIME'] == expected_date].sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)

    expected_data = [
        [expected_date, 'SPX', 6700.0, None, None, None, web_link],
        [expected_date, "SPX", 6685.0, None, None, None, web_link],
        [expected_date, "SPX", 6652.0, None, "first resistance", None, web_link],
        [expected_date, "SPX", 6639.0, 6644.0, None, None, web_link],
        [expected_date, "SPX", 6600.0, None, None, None, web_link],
        [expected_date, "SPX", 6555.0, None, None, None, web_link],
        [expected_date, "SPX", 6523.0, None, "21d EMA", None, web_link],
        [expected_date, "SPX", 6582.0, 6600.0, "9d EMA", "BUY", web_link],
        [expected_date, "SPX", 6549.0, 6555.0, None, "BUY", web_link],
        [expected_date, "SPX", 6677.0, 6700.0, None, "SELL", web_link],
    ]

    expected_df = _define_quant_dataframe(expected_data).sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(expected_df, quant_lvls_for_posts)
    except AssertionError as e:
        print("Missing from expected:\n", except_distinct(expected_df, quant_lvls_for_posts))
        print("Excess in target:\n", except_distinct(quant_lvls_for_posts, expected_df))
        raise e


def test_parse_quant_lvl_file_2(env_config, pipeline_data):
    """
    Check to see if it parses the quant_file properly
    This particular file has a typo
    :return:
    """
    web_link = "https://tradingedge.club/posts/88439857"

    df = pipeline_data["clean_df"]
    expected_date =  pd.Timestamp("2025-07-31")
    quant_lvls_for_posts = df[df['DATETIME'] == expected_date].sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)

    expected_data = [
        [expected_date, 'SPX', 6500.0, None, None, None, web_link],
        [expected_date, "SPX", 6444.0, None, "first major upside resistance", None, web_link],
        [expected_date, "SPX", 6412.0, None, None, None, web_link],
        [expected_date, "SPX", 6400.0, 6405, None, None, web_link],
        [expected_date, "SPX", 6345.0, None, "strong chance of reversal", None, web_link],
        [expected_date, "SPX", 6300.0, None, None, None, web_link],
        [expected_date, "SPX", 6369.0, 6375.0, "buy zone", "BUY", web_link],
        [expected_date, "SPX", 6320.0, 6345.0, None, "BUY", web_link],
        [expected_date, "SPX", 6467.0, 6475.0, "strong chance of reversal", "SELL", web_link]
    ]

    expected_df = _define_quant_dataframe(expected_data).sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(expected_df, quant_lvls_for_posts)
    except AssertionError as e:
        print("Missing from expected:\n", except_distinct(expected_df, quant_lvls_for_posts))
        print("Excess in target:\n", except_distinct(quant_lvls_for_posts, expected_df))
        raise e


def test_parse_quant_lvl_post_without_buy_sell_zones(env_config, pipeline_data):
    """
    Check to see if it parses the page properly
    :return:
    """
    web_link = "https://tradingedge.club/posts/86235552"

    df = pipeline_data["clean_df"]
    expected_date = pd.Timestamp("2025-06-17")
    quant_lvls_for_posts = df[df['DATETIME'] == expected_date].sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)

    expected_data = [
        [expected_date, "SPX", 6089.0, None,
         "If 6065 breaks, then expect this as a hard resistance. High chance of reversal form there.", None, web_link],
        [expected_date, "SPX", 6065.0, None, "top of the iron condor and  strong resistance", None, web_link],
        [expected_date, "SPX", 6035.0, None, None, None, web_link],
        [expected_date, "SPX", 6020.0, 6025.0, None, None, web_link],
        [expected_date, "SPX", 5996.0, None, None, None, web_link],
        [expected_date, "SPX", 5975.0, None, "high chance of reversal from here.", None, web_link],
        [expected_date, "SPX", 5965.0, None, None, None, web_link],
        [expected_date, "SPX", 5950.0, None, None, None, web_link],
        [expected_date, "SPX", 5925.0, None, None, None, web_link]
    ]

    expected_df = _define_quant_dataframe(expected_data).sort_values("START_LVL_PRICE",
                                                                               ascending=False).reset_index(drop=True)


    try:
        pd.testing.assert_frame_equal(expected_df, quant_lvls_for_posts)
    except AssertionError as e:
        print("Missing from expected:\n", except_distinct(expected_df, quant_lvls_for_posts))
        print("Excess in target:\n", except_distinct(quant_lvls_for_posts, expected_df))
        raise e



def except_distinct(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Returns rows in df1 that are not in df2, with duplicates removed from the result.
    This is the equivalent of a standard SQL EXCEPT operation.
    """
    # Use drop_duplicates() to handle multiplicity correctly for a standard EXCEPT.
    merged = df1.merge(df2.drop_duplicates(), how='left', indicator=True)

    # Filter for rows that only exist in the left DataFrame (df1).
    # Then drop the indicator column and any remaining duplicates to get a clean result.
    result = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')

    return result.drop_duplicates()


