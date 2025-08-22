import pandas as pd
import re
import logging

EXCHANGE_RATE = 16000  # $1 = Rp16.000
logger = logging.getLogger(__name__)

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        required_columns = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # 1. Hapus baris dengan data invalid
        df = df[
            (df['Title'].str.lower() != 'unknown product') &
            (df['Price'].str.lower() != 'price unavailable') &
            (df['Rating'].str.lower() != 'invalid rating')
        ].copy()

        # 2. Hapus baris kosong/null
        df.dropna(inplace=True)

        # 3. Hapus duplikat
        df.drop_duplicates(inplace=True)

        # 4. Bersihkan kolom Price → float64 (dalam Rupiah)
        df['Price'] = df['Price'].astype(str).str.replace(r'[^0-9.]', '', regex=True)
        df = df[df['Price'] != '']  # Hapus baris yang kosong setelah pembersihan
        df['Price'] = df['Price'].astype(float) * EXCHANGE_RATE

        # 5. Bersihkan kolom Rating → float64
        df['Rating'] = df['Rating'].astype(str).str.extract(r'(\d+\.\d+)')
        df['Rating'] = df['Rating'].astype(float)

        # 6. Bersihkan kolom Colors → int64
        df['Colors'] = df['Colors'].astype(str).str.extract(r'(\d+)')
        df['Colors'] = df['Colors'].astype(int)

        # 7. Bersihkan kolom Size → object
        df['Size'] = df['Size'].astype(str).str.replace("Size:", "").str.strip()

        # 8. Bersihkan kolom Gender → object
        df['Gender'] = df['Gender'].astype(str).str.replace("Gender:", "").str.strip()

        # 9. Pastikan tipe data akhir sesuai ketentuan
        df = df.astype({
            "Title": "object",
            "Price": "float64",
            "Rating": "float64",
            "Colors": "int64",
            "Size": "object",
            "Gender": "object"
        })

        return df

    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        raise
