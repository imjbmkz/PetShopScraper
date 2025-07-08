import asyncio
import math
import json
import pandas as pd

from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger


class TheRangeETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "TheRange"
        self.BASE_URL = "https://www.therange.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '#variant_container'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 5
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 10

    def extract(self, category):
        category_link = f"https://www.therange.co.uk{category}"
        urls = []
        soup = asyncio.run(self.scrape(category_link,  '#root',
                           min_sec=self.MIN_SEC_SLEEP_PRODUCT_INFO, max_sec=self.MAX_SEC_SLEEP_PRODUCT_INFO))
        n_product = int(soup.find('div', id="root")['data-total-results'])
        n_pagination = math.ceil(n_product / 24)

        for n in range(1, n_pagination + 1):
            pagination_url = category_link + \
                f"?sort=relevance&in_stock_f=true&page={n}"
            pagination_soup = asyncio.run(
                self.scrape(pagination_url, '#product-list', min_sec=self.MIN_SEC_SLEEP_PRODUCT_INFO, max_sec=self.MAX_SEC_SLEEP_PRODUCT_INFO))

            urls.extend([product.get('href') for product in pagination_soup.find_all(
                'a', class_="ProductCard-module__productTitle___fJH9Q")])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find('h1', id="product-dyn-title").get_text()
            product_description = soup.find(
                'p', id='product-dyn-desc').find(string=True)
            product_url = url.replace(self.BASE_URL, "")
            product_rating = "0/5"
            product_id = soup.find('input', id="product_id").get('value')
            clean_url = url.split('#')[0]

            if not soup.find('div', class_="no_reviews_info"):
                product_rating_soup = asyncio.run(self.extract_scrape_content(
                    f'{clean_url}?action=loadreviews&pid={product_id}&page=1', '#review-product-summary'))

                if product_rating_soup.find('div', id="review-product-summary"):
                    product_rating = str(round((int(product_rating_soup.find('div', id="review-product-summary").findAll(
                        'div', class_="progress-bar")[0].get('aria-valuenow')) / 100) * 5, 2)) + '/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            product_details = asyncio.run(
                self.get_json_product(f'{clean_url}?json'))
            if len(product_details['variant_arr']) > 1:
                for var_details in product_details['variant_arr']:
                    if " - " in var_details['name']:
                        variants.append(var_details['name'].split(" - ")[1])

                    if var_details['price_was'] == None:
                        prices.append(var_details['price'] / 100)
                        discounted_prices.append(None)
                        discount_percentages.append(None)

                    else:
                        prices.append(var_details['price_was'] / 100)
                        discounted_prices.append(var_details['price'] / 100)
                        discount_percentages.append(
                            var_details['price_was_percent'] / 100)

                    image_urls.append(
                        soup.find('meta', attrs={'property': "og:image"}).get('content'))

            else:
                variants.append(None)
                image_urls.append(
                    soup.find('meta', attrs={'property': "og:image"}).get('content'))
                if product_details['variant_arr'][0]['price_was'] == None:
                    prices.append(
                        product_details['variant_arr'][0]['price'] / 100)
                    discounted_prices.append(None)
                    discount_percentages.append(None)
                else:
                    prices.append(
                        product_details['variant_arr'][0]['price_was'] / 100)
                    discounted_prices.append(
                        product_details['variant_arr'][0]['price'] / 100)
                    discount_percentages.append(
                        product_details['variant_arr'][0]['price_was_percent'] / 100)

            df = pd.DataFrame({
                "variant": variants,
                "price": prices,
                "discounted_price": discounted_prices,
                "discount_percentage": discount_percentages,
                "image_urls": image_urls
            })
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
