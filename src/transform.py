import logging
import sys
from typing import List, Dict, Any, Optional
import datetime
import re

import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame

from config import Config

# Assuming your function is in a file named 'quant_logic.py' or similar.
# If it's in this file, you can paste it above.
# from quant_logic import retrieve_quant_levels


logger = logging.getLogger(__name__)

def run(config:Config, raw_posts_json: []) -> pd.DataFrame:
    """
    Take the unstructured json data from extract and normalizes it here into a structured df
    :param posts:
    :return:
    """
    quant_df_with_dupes = _parse_quant_levels_to_data(raw_posts_json)
    deduplicated_days_df = _deduplicate_days(quant_df_with_dupes)
    deduplicated_rows_df = _deduplicate_rows(config, deduplicated_days_df)
    return _clean_df(config, deduplicated_rows_df)

def _parse_quant_levels_to_data(posts: []) -> pd.DataFrame:
    """
    Parses 'quant_lvl_text' from a list of posts into a structured list of dictionaries
    ready for a Pandas DataFrame.
    """

    parsed_rows = []

    # Regex 1: Split sections by "---" (handling variation in dash count/spacing)
    section_split_pattern = re.compile(r'\n\s*-{3,}\s*\n?')

    # Regex 2: Line Parser
    # Group 1: Start Price /Group 2: End Price /Group 3: Comment (Optional)
    line_pattern = re.compile(r'^\s*(\d{4}(?:\.\d+)?)(?:\s*-\s*(\d{4}(?:\.\d+)?))?\s*(.*)')

    all_posts_with_quant_lvl = [post for post in posts if post.get('quant_lvl_text')]

    for post in all_posts_with_quant_lvl:
        date_of_post = post.get('date_posted')

        logging.info(f"Parsing post: {date_of_post}:{post.get('title')}")
        # DATETIME COL
        date_of_post = post.get('date_posted')
        date_val = datetime.datetime.fromisoformat(date_of_post.replace("Z", "+00:00"))

        # 1. Split text into sections based on '---'
        raw_text = post.get('quant_lvl_text')
        sections = section_split_pattern.split(raw_text)

        # 2. Iterate through sections and assign Buy/Sell indicator
        for i, section_content in enumerate(sections):

            # Section 0 = First block /  Section 1 = Second block / Section 2 = Third block
            if i == 0:
                buy_sell_ind = None
            elif i == 1:
                buy_sell_ind = "BUY"
            elif i == 2:
                buy_sell_ind = "SELL"
            else:
                buy_sell_ind = None  # Fallback for unexpected extra sections

            # 3. Process lines within this section
            lines = section_content.strip().split('\n')

            for line in lines:
                clean_line = line.strip()
                if not clean_line:
                    continue

                match = line_pattern.match(clean_line)    # Match the line against the price regex

                if match:
                    #FIRST_PRICE_LVL COL
                    price_start = float(match.group(1))
                    #SECOND_PRICE_LVL COL (OPTIONAL)
                    price_end = float(match.group(2)) if match.group(2) else None

                    # COMMENT COL (OPTIONAL)
                    comment = match.group(3).strip()
                    if comment:
                        # Remove leading separators like ": " or "- " from the comment
                        comment = re.sub(r'^[:\-\s]+', '', comment)
                    else: # Store None if comment is empty string
                        comment = None

                    # Build the row
                    row = {
                        "DATETIME": date_val,
                        "TICKER": "SPX",  # Defaulting to SPX as context implies index levels
                        "START_LVL_PRICE": price_start,
                        "END_LVL_PRICE": price_end,
                        "COMMENTS": comment,
                        "BUY_SELL_IND": buy_sell_ind,
                        "WEB_LINK": post.get('link')
                    }

                    parsed_rows.append(row)

    return _define_quant_dataframe(parsed_rows)

def _define_quant_dataframe(parsed_data: []) -> pd.DataFrame:
    """
    Converts a list of parsed quant level dictionaries into a pandas DataFrame.
    Enforces types for Datetime and Float columns.

    :param parsed_data: List of dicts, typically output from parse_quant_levels_to_data()
    :return: pd.DataFrame
    """
    logging.info("Deduplicating Days for df...")

    # 1. Create DataFrame from list of dicts
    df = pd.DataFrame(parsed_data)
    df.columns = [
            "DATETIME", "TICKER", "START_LVL_PRICE","END_LVL_PRICE", "COMMENTS", "BUY_SELL_IND", "WEB_LINK"
        ]

    # 2. Check if data exists to avoid errors on empty lists
    if df.empty:
        logging.error("WARNING: Parsing into data returned nothing. Double check if data is parsed correctly")
        sys.exit(1)

    # 3. Enforce Data Types
    # Convert DATETIME column to actual datetime objects

    df['DATETIME'] = pd.to_datetime(df['DATETIME'], errors='coerce')
    df['START_LVL_PRICE'] = df['START_LVL_PRICE'].astype(float)
    df['END_LVL_PRICE'] = df['END_LVL_PRICE'].astype(float)
    df['COMMENTS'] = df['COMMENTS'].str.replace('\xa0', ' ')
    #rest are string

    return df


def _deduplicate_days(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the DataFrame to keep only the records associated with the
    LATEST datetime for each calendar date.

    Uses a vectorized 'transform' (SQL Window Function equivalent) for efficiency.
    """
    logging.info("Deduplicating Rows for df...")
    if df.empty:
        return df

    # 1. Create a temporary date column for grouping
    df['temp_date'] = df['DATETIME'].dt.date

    # 2. Calculate the Window Function
    # SQL Equivalent: MAX(DATETIME) OVER (PARTITION BY temp_date)
    df['latest_datetime_of_day'] = df.groupby('temp_date')['DATETIME'].transform('max')

    # 3. Filter
    deduped_df = df[df['DATETIME'] == df['latest_datetime_of_day']].copy()
    deduped_df = deduped_df.drop(columns=['temp_date', 'latest_datetime_of_day'])


    return deduped_df


def _deduplicate_rows(config:Config, df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicates a DataFrame based on a primary key of (DATETIME, TICKER, START_LVL_PRICE).

    Merge Logic:
    1. Group by primary key.
    2. For numeric columns (like END_LVL_PRICE): take the first non-null value.
    3. For string columns (COMMENTS, WEB_LINK, etc.):
       - Ignore nulls.
       - Concatenate distinct, non-empty strings with ' | '.
       - If only one unique value exists, keep that value.
    """
    if df.empty:
        return df

    #normalize
    df['DATETIME'] = pd.to_datetime(df['DATETIME'], utc=True).dt.tz_localize(None)
    df['DATETIME'] = df['DATETIME'].dt.floor('1s')
    df['START_LVL_PRICE'] = pd.to_numeric(df['START_LVL_PRICE'], errors='coerce')
    df['START_LVL_PRICE'] = df['START_LVL_PRICE'].round(2)
    df['TICKER'] = df['TICKER'].astype(str).str.strip()

    primary_key = config.oracle_quant_pks
    deduped_df = df.groupby(primary_key, as_index=False, dropna=False).agg(merge_logic)

    return deduped_df

# Define the custom aggregation function
def merge_logic(series):
    # 1. Drop NA values
    valid_values = series.dropna()

    # 2. If no valid values, return None (or NaN)
    if valid_values.empty:
        return None

    # 3. Handle Numeric Columns (e.g. END_LVL_PRICE)
    # We take the first valid value found (max() or min() also works if preference exists)
    if pd.api.types.is_numeric_dtype(series):
        return valid_values.iloc[0]

    # 4. Handle String/Object Columns
    else:
        # Convert to string, strip whitespace, and get unique values
        unique_vals = sorted(set(str(v).strip() for v in valid_values if str(v).strip()))

        # If nothing remains after stripping, return None
        if not unique_vals:
            return None

        # Concatenate unique strings
        return " | ".join(unique_vals)

def _clean_df(config:Config, df: pd.DataFrame) -> DataFrame:
    """
    :param df:
    :return:
    """

    #normalize timestamps
    df['DATETIME'] = pd.to_datetime(df['DATETIME'],
                                            format='%Y-%m-%d').dt.normalize().dt.tz_localize(None)

    #check for duplicates based off of pk
    pks = config.oracle_quant_pks
    if df.duplicated(subset=pks).any():
        logging.error("Integrity Error: Duplicate keys found.")

    if df[pks].isnull().any().any():
        logging.error("Integrity Error: PK columns contain Nulls.")

    return df



