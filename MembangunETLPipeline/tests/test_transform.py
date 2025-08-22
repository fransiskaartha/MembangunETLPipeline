import unittest
import pandas as pd
from utils.transform import transform_data

class TestTransform(unittest.TestCase):

    def setUp(self):
        self.raw_data = pd.DataFrame({
            "Title": ["T-shirt", "Unknown Product"],
            "Price": ["$100.00", "Price Unavailable"],
            "Rating": ["⭐ 4.5 / 5", "Invalid Rating"],
            "Colors": ["3 Colors", "Unknown"],
            "Size": ["Size: M", "Unknown"],
            "Gender": ["Gender: Women", "Unknown"],
            "timestamp": ["2025-05-14 10:00:00", "2025-05-14 10:01:00"]
        })

    def test_transform_removes_invalid(self):
        df_cleaned = transform_data(self.raw_data)
        self.assertEqual(len(df_cleaned), 1)

    def test_price_converted_to_rupiah(self):
        df_cleaned = transform_data(self.raw_data)
        self.assertEqual(df_cleaned.iloc[0]["Price"], 100.00 * 16000)

    def test_rating_converted_to_float(self):
        df_cleaned = transform_data(self.raw_data)
        self.assertAlmostEqual(df_cleaned.iloc[0]["Rating"], 4.5)

    def test_colors_converted_to_int(self):
        df_cleaned = transform_data(self.raw_data)
        self.assertEqual(df_cleaned.iloc[0]["Colors"], 3)

    def test_size_and_gender_cleaned(self):
        df_cleaned = transform_data(self.raw_data)
        self.assertEqual(df_cleaned.iloc[0]["Size"], "M")
        self.assertEqual(df_cleaned.iloc[0]["Gender"], "Women")

    def test_transform_handles_missing_columns(self):
        bad_df = pd.DataFrame({"Foo": [1], "Bar": [2]})
        with self.assertRaises(ValueError) as context:
            transform_data(bad_df)
        self.assertIn("Missing required column", str(context.exception))


if __name__ == '__main__':
    unittest.main()
