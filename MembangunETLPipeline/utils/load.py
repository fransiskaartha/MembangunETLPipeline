import pandas as pd
import os
import logging
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_to_csv(df: pd.DataFrame, output_path: str = "products.csv", raise_on_error=False):
    if not isinstance(df, pd.DataFrame):
        logger.error("Gagal menyimpan data: Input harus berupa pandas DataFrame.")
        if raise_on_error:
            raise ValueError("Input harus berupa pandas DataFrame.")
        return False

    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Data berhasil disimpan ke {output_path}")
        logger.info(f"Kolom yang disimpan: {list(df.columns)}")
        return True
    except Exception as e:
        logger.error(f"Gagal menyimpan data: {e}")
        if raise_on_error:
            raise
        return False

def load_to_gsheet(df: pd.DataFrame, sheet_name: str, json_keyfile: str):
    if not isinstance(df, pd.DataFrame):
        logger.error("Gagal menyimpan ke Google Sheets: Input bukan DataFrame")
        return None

    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_file(json_keyfile, scopes=scope)
        client = gspread.authorize(credentials)

        spreadsheet = client.create(sheet_name)
        spreadsheet.share(None, perm_type='anyone', role='writer')

        worksheet = spreadsheet.sheet1
        set_with_dataframe(worksheet, df)

        logger.info(f"Data berhasil disimpan ke Google Sheets: {spreadsheet.url}")
        return spreadsheet.url
    except Exception as e:
        logger.error(f"Gagal menyimpan ke Google Sheets: {e}")
        return None

def load_to_postgresql(df: pd.DataFrame, db_name: str, user: str, password: str, host: str, port: str):
    if not isinstance(df, pd.DataFrame):
        logger.error("Gagal menyimpan ke PostgreSQL: Input bukan DataFrame")
        return False

    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                title TEXT,
                price FLOAT,
                rating FLOAT,
                colors INTEGER,
                size TEXT,
                gender TEXT,
                timestamp TEXT
            )
        """)

        cursor.execute("TRUNCATE TABLE products RESTART IDENTITY")

        for _, row in df.iterrows():
            timestamp_value = row.get("timestamp")

            # Jika timestamp kosong, gunakan waktu sekarang
            if pd.isna(timestamp_value):
                timestamp_value = pd.to_datetime('now').strftime('%Y-%m-%d %H:%M:%S')  # Format timestamp sekarang

            cursor.execute("""
                INSERT INTO products (title, price, rating, colors, size, gender, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get("Title"),
                row.get("Price"),
                row.get("Rating"),
                row.get("Colors"),
                row.get("Size"),
                row.get("Gender"),
                timestamp_value
            ))

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Data berhasil disimpan ke PostgreSQL.")
        return True
    except Exception as e:
        logger.error(f"Gagal menyimpan ke PostgreSQL: {e}")
        return False
    
if __name__ == "__main__":
    input_path = "cleaned_products.csv"
    json_key = "google-sheets-api.json"

    if not os.path.exists(input_path):
        logger.error(f"File '{input_path}' tidak ditemukan.")
    else:
        df = pd.read_csv(input_path)

        load_to_csv(df)
        url = load_to_gsheet(df, "ETL-Fashion-Studio", json_key)
        if url:
            logger.info(f"Google Sheets URL: {url}")

        load_to_postgresql(
            df,
            db_name="etl_fashion",
            user="postgres",
            password="new_password",
            host="localhost",
            port="5432"
        )
