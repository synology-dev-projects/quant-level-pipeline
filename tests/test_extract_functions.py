from datetime import datetime, timezone
import pytest
import common_lib.config.main_config as config
from extract import _get_file_content
import json


@pytest.mark.integration
def test_extract_has_file_property(env_config, pipeline_data):
    """
    """
    processed_feed = pipeline_data["raw_post_json"]
    posts_with_files = [p for p in processed_feed if p.get("file_link") is not None]

    assert len(posts_with_files) == 22, (
        f"Expected 21 posts with files, but found {len(posts_with_files)}"
    )

    target_link_has_file = "https://tradingedge.club/posts/88439857"
    target_link_no_file = "https://tradingedge.club/posts/95184701"
    target_post_has_file = next((p for p in processed_feed if p["link"] == target_link_has_file), None)
    target_post_no_file = next((p for p in processed_feed if p["link"] == target_link_no_file), None)
    expected_file_url = "https://media2-production.mightynetworks.com/asset/ec06ea6e-f031-41dd-a77a-29b40f43e2f9/Untitled_document-5.txt"

    assert target_post_has_file["file_link"] == expected_file_url, (
        f"Expected {expected_file_url}, but got {target_post_has_file['file_link']}"
    )

    assert target_post_no_file["file_link"] is None, (
        f"Expected {target_link_no_file} to not have file, but got {target_post_no_file['file_link']}"
    )


@pytest.mark.integration
def test_get_file_content():
    file_url = "https://media2-production.mightynetworks.com/asset/ec06ea6e-f031-41dd-a77a-29b40f43e2f9/Untitled_document-5.txt"
    file_content = _get_file_content(file_url)
    assert file_content is not None


@pytest.mark.integration
def test_extract_quant_levels_from_post_body(env_config, pipeline_data):
    results = pipeline_data["raw_post_json"]

    matches = [p for p in results if p['quant_lvl_text']]
    non_matches = [p for p in results if not p['quant_lvl_text']]

    assert all(post['quant_lvl_text'] for post in matches), "Found a post in matches with empty/None quant_lvl_text"
    assert all(not post['quant_lvl_text'] for post in non_matches), "Found a post in non-matches with non-empty text"
