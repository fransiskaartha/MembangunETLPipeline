"""
Module untuk melakukan ekstraksi data dari website Fashion Studio
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# URL target untuk scraping
BASE_URL = "https://fashion-studio.dicoding.dev"


def create_session() -> requests.Session:
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        return session
    except Exception as e:
        logger.error(f"Error creating session: {e}")
        raise


def get_page_content(session: requests.Session, page: int = 1) -> Optional[str]:
    try:
        url = BASE_URL if page == 1 else f"{BASE_URL}/page{page}"
        logger.info(f"Attempting to fetch: {url}")
        response = session.get(url, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully fetched page {page}, status code: {response.status_code}")
        return response.text
    except requests.RequestException as e:
        logger.error(f"Error fetching page {page}: {e}")
        return None


def parse_product_card(card) -> Dict[str, Union[str, float]]:
    product = {}
    product["timestamp"] = datetime.now().isoformat()

    try:
        product_details = card.select_one(".product-details")
        if product_details:
            title_element = product_details.select_one("h3")
            product["Title"] = title_element.text.strip() if title_element else "Unknown Product"

            product["Rating"] = "Invalid Rating"
            product["Colors"] = "Unknown"
            product["Size"] = "Unknown"
            product["Gender"] = "Unknown"

            # Ambil semua <p> tag
            info_elements = product_details.find_all("p")
            for info in info_elements:
                info_text = info.text.strip()
                if "Rating:" in info_text:
                    product["Rating"] = info_text.split("Rating:")[-1].strip()
                elif "Color" in info_text:
                    product["Colors"] = info_text
                elif "Size:" in info_text:
                    product["Size"] = info_text.split("Size:")[-1].strip()
                elif "Gender:" in info_text:
                    product["Gender"] = info_text.split("Gender:")[-1].strip()

            # Ambil harga dari .price-container .price
            price_element = card.select_one(".price-container .price")
            if price_element:
                product["Price"] = price_element.text.strip()
            else:
                product["Price"] = "Price Unavailable"

        else:
            logger.warning("Could not find product-details.")
            product["Title"] = "Unknown Product"
            product["Price"] = "Price Unavailable"
            product["Rating"] = "Invalid Rating"
            product["Colors"] = "Unknown"
            product["Size"] = "Unknown"
            product["Gender"] = "Unknown"

    except Exception as e:
        logger.error(f"Error parsing product card: {e}")
        product["Title"] = product.get("Title", "Unknown Product")
        product["Price"] = product.get("Price", "Price Unavailable")
        product["Rating"] = product.get("Rating", "Invalid Rating")
        product["Colors"] = product.get("Colors", "Unknown")
        product["Size"] = product.get("Size", "Unknown")
        product["Gender"] = product.get("Gender", "Unknown")

    return product



def extract_products_from_page(html_content: str) -> List[Dict[str, Union[str, float]]]:
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        product_cards = soup.select(".collection-card")
        logger.info(f"Found {len(product_cards)} product cards on page")

        products = []
        for card in product_cards:
            product = parse_product_card(card)
            products.append(product)

        return products
    except Exception as e:
        logger.error(f"Error parsing HTML content: {e}")
        return []


def scrape_all_pages(max_pages: int = 50) -> pd.DataFrame:
    all_products = []
    session = create_session()

    try:
        for page in range(1, max_pages + 1):
            logger.info(f"Scraping page {page} of {max_pages}")
            html_content = get_page_content(session, page)

            if not html_content:
                logger.warning(f"Failed to get content from page {page}")
                for retry in range(3):
                    logger.info(f"Retrying page {page} (attempt {retry+1}/3)")
                    time.sleep(2)
                    html_content = get_page_content(session, page)
                    if html_content:
                        break

                if not html_content:
                    logger.error(f"Failed to get content from page {page} after retries")
                    continue

            if page == 1:
                with open(f"debug_page_{page}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)

            products = extract_products_from_page(html_content)
            all_products.extend(products)
            logger.info(f"Found {len(products)} products on page {page}")
            time.sleep(1)

    except Exception as e:
        logger.error(f"Error scraping all pages: {e}")
    finally:
        session.close()

    if all_products:
        df = pd.DataFrame(all_products)
        logger.info(f"Total products scraped: {len(df)}")
        return df
    else:
        logger.warning("No products were scraped!")
        return pd.DataFrame()


def main():
    try:
        df = scrape_all_pages()
        df.to_csv("raw_products.csv", index=False)
        logger.info("Extraction process completed successfully")
        return df
    except Exception as e:
        logger.error(f"Error in extraction process: {e}")
        raise


if __name__ == "__main__":
    main()
