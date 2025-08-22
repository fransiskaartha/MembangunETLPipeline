import unittest
import pandas as pd
from utils import extract
from unittest.mock import patch

class TestExtract(unittest.TestCase):

    def test_scrape_returns_dataframe(self):
        df = extract.scrape_all_pages(max_pages=1)
        self.assertIsInstance(df, pd.DataFrame)

    def test_scraped_data_not_empty(self):
        df = extract.scrape_all_pages(max_pages=1)
        self.assertGreater(len(df), 0)

    def test_required_columns_exist(self):
        df = extract.scrape_all_pages(max_pages=1)
        for col in ["Title", "Price", "Rating", "Colors", "Size", "Gender", "timestamp"]:
            self.assertIn(col, df.columns)

    def test_timestamp_can_be_converted(self):
        df = extract.scrape_all_pages(max_pages=1)
        try:
            pd.to_datetime(df['timestamp'])
        except Exception:
            self.fail("Format timestamp salah!")

    @patch("utils.extract.get_page_content", return_value=None)
    def test_extract_handles_failed_request(self, mock_get_page):
        df = extract.scrape_all_pages(max_pages=1)
        self.assertTrue(df.empty)

if __name__ == '__main__':
    unittest.main()