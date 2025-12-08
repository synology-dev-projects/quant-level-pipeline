import logging
import pytest


def test_sample():
    # 1. Run the code that prints
    logging.info("Hello, Vancouver!")
    logging.error("Sample test failed")
    assert True