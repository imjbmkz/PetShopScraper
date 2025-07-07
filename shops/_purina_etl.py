import asyncio
import pandas as pd
from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger


class PurinaETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "Purina"
        self.BASE_URL = "https://www.purina.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = ''
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 2
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 5

    def extract(self, category):
        current_url = f"{self.BASE_URL}{category}"
        page = 0
        urls = []

        while True:
            soup = asyncio.run(self.scrape(current_url, '.main-view-content'))

            if soup:
                new_urls = soup.select("a[class*='product-tile_image']")
                if new_urls:
                    new_urls = [u["href"] for u in new_urls]
                    new_urls = [self.BASE_URL+u for u in new_urls]
                    urls.extend(new_urls)

                    page += 1
                    current_url = f"{current_url}?page={page}"
                    continue
                break

            else:
                break

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)

        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="dsu-product--title").get_text(strip=True)
            product_url = url.replace(self.BASE_URL, "")
            product_description = soup.find(
                'meta', attrs={'property': 'og:description'}).get('content')
            product_rating = '0/5'

            rating_wrapper = soup.find(
                'div', attrs={'class': ['review-stats test1']})
            if rating_wrapper:
                product_rating = rating_wrapper.find(
                    'div', class_='count').getText(strip=True)

            variants = [None]
            prices = [None]
            discounted_prices = [None]
            discount_percentages = [None]

            image_urls = [', '.join([self.BASE_URL + img.find('img').get('src') for img in soup.find(
                'div', class_="carousel-media").find_all('div', class_="field__item")])]
            df = pd.DataFrame(
                {
                    "variant": variants,
                    "price": prices,
                    "discounted_price": discounted_prices,
                    "discount_percentage": discount_percentages,
                    "image_urls": image_urls
                }
            )

            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
