import pandas as pd
from utils.extract import scrape_all_pages
from utils.transform import transform_data
from utils.load import load_to_csv, load_to_gsheet, load_to_postgresql

def main():
    print("\nMemulai ETL Pipeline...")

    # 1. Extract
    print("\nTahap Extract...")
    try:
        raw_df = scrape_all_pages(max_pages=50)
        raw_df.to_csv("raw_products.csv", index=False)
        print(f"Data mentah disimpan ke raw_products.csv (total {len(raw_df)} data)")
    except Exception as e:
        print(f"Gagal extract data: {e}")
        return

    # 2. Transform
    print("\nTransform...")
    try:
        cleaned_df = transform_data(raw_df)
        cleaned_df.to_csv("cleaned_products.csv", index=False)
        print(f"Data sudah dibersihkan dan disimpan ke cleaned_products.csv (total {len(cleaned_df)} data)")
    except Exception as e:
        print(f"Gagal transform data: {e}")
        return

    # 3. Load
    print("\nLoading...")
    try:
        # Load ke CSV
        load_to_csv(cleaned_df, output_path="products.csv")
        print("Disimpan ke products.csv")

        # Load ke Google Sheets
        json_key = "google-sheets-api.json"
        gsheet_url = load_to_gsheet(cleaned_df, "ETL-Fashion-Studio", json_key)
        if gsheet_url:
            print(f"Google Sheets URL: {gsheet_url}")
        else:
            print("Gagal menyimpan ke Google Sheets")

        # Load ke PostgreSQL
        pg_status = load_to_postgresql(
            cleaned_df,
            db_name="etl_fashion",
            user="postgres",
            password="new_password",
            host="localhost",
            port="5432"
        )
        if pg_status:
            print("Data berhasil disimpan ke PostgreSQL")
        else:
            print("Gagal menyimpan ke PostgreSQL")

    except Exception as e:
        print(f"Gagal load data: {e}")
        return

    print("\nETL Pipeline selesai")


if __name__ == "__main__":
    main()
