from datetime import datetime, timezone

import pytest

from config import load_config
from extract import _fetch_raw_feed, _extract_file_link, _parse_feed_data, _get_file_content, \
    _extract_quant_levels_from_post_body
import json

def test_extract_has_file_property(env_config, pipeline_data):
    """
    """
    processed_feed = pipeline_data["raw_post_json"]
    # 1. Assert that the total number of posts with files is 22
    # We filter the list for items where 'has_file' is True and count them
    posts_with_files = [p for p in processed_feed if p.get("file_link") is not None]
    # --- PRINT LOGIC ---

    # print("POST WITH FILES:\n",json.dumps(posts_with_files, indent=2))
    # -------------------


    assert len(posts_with_files) == 22, (
        f"Expected 21 posts with files, but found {len(posts_with_files)}"
    )

    # 2. Assert that the specific target post is marked correctly
    target_link_has_file = "https://tradingedge.club/posts/88439857"
    target_link_no_file = "https://tradingedge.club/posts/95184701"
    # Search for the post in the processed list
    target_post_has_file = next((p for p in processed_feed if p["link"] == target_link_has_file), None)
    target_post_no_file = next((p for p in processed_feed if p["link"] == target_link_no_file), None)
    expected_file_url = "https://media2-production.mightynetworks.com/asset/ec06ea6e-f031-41dd-a77a-29b40f43e2f9/Untitled_document-5.txt"

    # Verify the post exists and the flag is True
    assert target_post_has_file["file_link"] == expected_file_url, (
        f"Expected {expected_file_url}, but got {target_post_has_file['file_link']}"
    )

    # Verify the post exists and the flag is True
    assert target_post_no_file["file_link"] is None, (
        f"Expected {target_link_no_file} to not have file, but got {target_post_no_file['file_link']}"
    )

def test_get_file_content():
    file_url = "https://media2-production.mightynetworks.com/asset/ec06ea6e-f031-41dd-a77a-29b40f43e2f9/Untitled_document-5.txt"
    file_content=_get_file_content(file_url)
    assert file_content is not None

def test_extract_quant_levels_from_post_body(env_config, pipeline_data):
    results = pipeline_data["raw_post_json"]

    # C. Print Results
    matches = [p for p in results if p['quant_lvl_text']]
    non_matches = [p for p in results if not p['quant_lvl_text']]

    assert all(post['quant_lvl_text'] for post in matches), "Found a post in matches with empty/None quant_lvl_text"
    assert all(not post['quant_lvl_text'] for post in non_matches), "Found a post in non-matches with non-empty text"



