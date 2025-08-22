import unittest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from utils.load import load_to_csv, load_to_postgresql, load_to_gsheet


class TestLoad(unittest.TestCase):

    def setUp(self):
        self.test_file = "test_output.csv"
        self.df = pd.DataFrame({
            "Title": ["T-shirt"],
            "Price": [1600000.0],
            "Rating": [4.5],
            "Colors": [3],
            "Size": ["M"],
            "Gender": ["Women"],
            "timestamp": ["2025-05-14 10:00:00"]
        })

    def tearDown(self):
        for file in ["test_output.csv", "test_existing.csv", "error.csv"]:
            if os.path.exists(file):
                os.remove(file)

    def test_csv_file_created(self):
        result = load_to_csv(self.df, output_path=self.test_file)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.test_file))

    def test_csv_content_matches(self):
        load_to_csv(self.df, output_path=self.test_file)
        df_loaded = pd.read_csv(self.test_file)
        pd.testing.assert_frame_equal(self.df, df_loaded)

    def test_load_raises_exception_on_invalid_path(self):
        with self.assertRaises(Exception):
            load_to_csv(self.df, output_path="/invalid_path/test.csv", raise_on_error=True)

    def test_load_to_existing_file(self):
        path = "test_existing.csv"
        result = load_to_csv(self.df, output_path=path)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(path))

    def test_load_to_csv_with_invalid_df(self):
        with self.assertRaises(Exception):
            load_to_csv("bukan_df", output_path="error.csv", raise_on_error=True)

    @patch("utils.load.psycopg2.connect")
    def test_load_to_postgresql_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        result = load_to_postgresql(
            self.df,
            db_name="test_db",
            user="user",
            password="pass",
            host="localhost",
            port="5432"
        )

        self.assertTrue(result)
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("utils.load.psycopg2.connect", side_effect=Exception("DB Error"))
    def test_load_to_postgresql_failure(self, mock_connect):
        result = load_to_postgresql(
            self.df,
            db_name="wrong_db",
            user="user",
            password="wrong",
            host="localhost",
            port="5432"
        )
        self.assertFalse(result)

    @patch("utils.load.set_with_dataframe")
    @patch("utils.load.gspread.authorize")
    @patch("utils.load.Credentials.from_service_account_file")
    def test_load_to_gsheet_success(self, mock_creds, mock_authorize, mock_set_with_df):
        mock_client = MagicMock()
        mock_sheet = MagicMock()
        mock_spreadsheet = MagicMock()

        mock_spreadsheet.sheet1 = mock_sheet
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/test-url"
        mock_client.create.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client
        mock_creds.return_value = MagicMock()

        result = load_to_gsheet(self.df, "TestSheet", "google-sheets-api.json")
        self.assertIsInstance(result, str)
        self.assertIn("http", result)
        mock_set_with_df.assert_called_once()

    @patch("utils.load.gspread.authorize", side_effect=Exception("GS Error"))
    def test_load_to_gsheet_failure(self, mock_authorize):
        result = load_to_gsheet(self.df, "FailSheet", "google-sheets-api.json")
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
