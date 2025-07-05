import math
import asyncio
import requests
import pandas as pd

from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger


class FarmAndPetPlaceETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "FarmAndPetPlace"
        self.BASE_URL = "https://www.farmandpetplace.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '.content-page'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 2
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 5

    def extract(self, category):
        url = self.BASE_URL+category
        soup = asyncio.run(self.scrape(
            url, '.woocommerce-result-count', wait_for_network=True))
        n_product = [int(word) for word in soup.find(
            'p', class_="woocommerce-result-count").get_text().split() if word.isdigit()][0]
        n_pagination = math.ceil(n_product / 24)

        urls = []
        if n_pagination == 1:
            urls.extend([self.BASE_URL + product.find('a').get('href') for product in soup.find(
                'div', class_="shop-filters-area").find_all('div', class_="product")])
        else:
            for i in range(1, n_pagination + 1):
                base = url.split("page-")[0]
                new_url = f"{base}page-{i}.html"
                soup_pagination = asyncio.run(
                    self.scrape(new_url, '.shop-filters-area', wait_for_network=True))

                urls.extend([self.BASE_URL + product.find('a').get('href') for product in soup_pagination.find(
                    'div', class_="shop-filters-area").find_all('div', class_="product")])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', attrs={'itemprop': 'name'}).get_text()
            product_description = None

            if soup.find('div', class_="short-description"):
                product_description = soup.find(
                    'div', class_="short-description").get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_id = soup.find(
                'div', class_="ruk_rating_snippet").get('data-sku')

            rating_wrapper = requests.get(
                f"https://api.feefo.com/api/10/reviews/summary/product?since_period=ALL&parent_product_sku={product_id}&merchant_identifier=farm-pet-place&origin=www.farmandpetplace.co.uk")
            rating = float(rating_wrapper.json()['rating']['rating'])
            product_rating = f'{rating}/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            if soup.find('select', id="attribute"):
                variants.append(soup.find('select', id="attribute").find_all(
                    'option')[0].get('value'))
                image_urls.append(
                    soup.find('img', class_="attachment-shop_single").get('src'))
                if soup.find('div', class_="price").find('span', class_="rrp"):
                    price = float(soup.find('div', class_="price").find(
                        'span', class_="rrp").find('strong').get_text().replace('£', ''))
                    discounted_price = float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', ''))
                    discount_percentage = "{:.2f}".format(
                        (price - discounted_price) / price)

                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percentage)

                else:
                    prices.append(float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', '')))
                    discounted_prices.append(None)
                    discount_percentages.append(None)

            else:
                variants.append(None)
                image_urls.append(
                    soup.find('img', class_="attachment-shop_single").get('src'))
                if soup.find('div', class_="price").find('span', class_="rrp"):
                    price = float(soup.find('div', class_="price").find(
                        'span', class_="rrp").find('strong').get_text().replace('£', ''))
                    discounted_price = float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', ''))
                    discount_percentage = "{:.2f}".format(
                        (price - discounted_price) / price)

                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percentage)

                else:
                    prices.append(float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', '')))
                    discounted_prices.append(None)
                    discount_percentages.append(None)

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
