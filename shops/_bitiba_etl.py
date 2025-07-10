import re
import json
import math
import time
import random
import pandas as pd
import requests

from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger


class BitibaETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "Bitiba"
        self.BASE_URL = "https://www.bitiba.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = 'main#page-content'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 350
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 400

    def extract(self, category):
        scrape_url = f"https://www.bitiba.co.uk/api/discover/v1/products/list-faceted-partial?&path={category}&domain=bitiba.co.uk&language=en&page=1&size=24&ab=shop-10734_shop_product_catalog_api_enabled_targeted_delivery.enabled%2Bidpo-1141_article_based_product_cards_targeted_delivery.on%2Bshop-11393_disable_plp_spc_api_cache_targeted_delivery.on%2Bshop-11371_enable_sort_by_unit_price_targeted_delivery.on%2Bidpo-1390_rebranding_foundation_targeted_delivery.on%2Bexplore-3092-price-redesign_targeted_delivery.on"
        logger.info(f"Accessing in : {scrape_url}")
        urls = []
        response_data_product = requests.get(scrape_url)
        product_data = response_data_product.json()

        n_product = product_data['pagination']['count']
        n_pagination = math.ceil(n_product / 24)

        sleep_time = random.uniform(10, 15)
        logger.info(f"Sleeping in : {sleep_time} seconds")
        time.sleep(sleep_time)

        for n in range(1, n_pagination + 1):
            pagination_url = f"https://www.bitiba.co.uk/api/discover/v1/products/list-faceted-partial?&path={category}&domain=bitiba.co.uk&language=en&page={n}&size=24&ab=shop-10734_shop_product_catalog_api_enabled_targeted_delivery.enabled%2Bidpo-1141_article_based_product_cards_targeted_delivery.on%2Bshop-11393_disable_plp_spc_api_cache_targeted_delivery.on%2Bshop-11371_enable_sort_by_unit_price_targeted_delivery.on%2Bidpo-1390_rebranding_foundation_targeted_delivery.on%2Bexplore-3092-price-redesign_targeted_delivery.on"
            logger.info(f"Accessing in : {pagination_url}")
            response_pagination_product = requests.get(pagination_url)
            data_product = response_pagination_product.json()
            urls.extend([self.BASE_URL + product['path']
                        for product in data_product['productList']['products']])

            logger.info(f"Sleeping in : {sleep_time} seconds")

            time.sleep(random.uniform(10, 15))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)

        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_data_list = soup.select(
                "script[type*='application/ld+json']")
            if product_data_list:
                product_data = json.loads(product_data_list[0].text)

                product_title = product_data["name"]
                rating = '0/5'
                if "aggregateRating" in product_data.keys():
                    rating = product_data["aggregateRating"]["ratingValue"]
                    rating = f"{rating}/5"

                description = product_data["description"]
                product_url = url.replace(self.BASE_URL, "")

                # Placeholder for variant details
                variants = []
                prices = []
                discounted_prices = []
                discount_percentages = []
                image_urls = []

                pattern = r"^.*£"
                rrb_pattern = r"[^\d\.]"

                variants_list = soup.find(
                    'div', class_="VariantList_variantList__PeaNd")
                if variants_list:
                    variant_hopps = variants_list.select(
                        "div[data-hopps*='Variant']")
                    for variant_hopp in variant_hopps:

                        variant = variant_hopp.select_one(
                            "span[class*='VariantDescription_description']").text
                        image_variant = variant_hopp.find('img').get('src')
                        discount_checker = variant_hopp.find(
                            'div', class_="z-product-price__note-wrap")

                        if discount_checker:
                            price = float(re.sub(rrb_pattern, "", variant_hopp.select_one(
                                "div[class*='z-product-price__nowrap']").text))
                            discounted_price = float(re.sub(pattern, "", variant_hopp.select_one(
                                "span[class*='z-product-price__amount']").text))
                            discount_percent = round(
                                (price - float(discounted_price)) / price, 2)
                        else:
                            price = float(re.sub(pattern, "", variant_hopp.select_one(
                                "span[class*='z-product-price__amount']").text))
                            discounted_price = None
                            discount_percent = None

                        variants.append(variant)
                        prices.append(price)
                        discounted_prices.append(discounted_price)
                        discount_percentages.append(discount_percent)
                        image_urls.append(image_variant)

                else:
                    variant = soup.select_one(
                        "div[data-zta*='ProductTitle__Subtitle']").text
                    discount_checker = soup.find('span', attrs={
                                                 'data-zta': 'SelectedArticleBox__TopSection'}).find('div', class_="z-product-price__note-wrap")

                    if discount_checker:
                        price = float(re.sub(rrb_pattern, "", soup.find('span', attrs={
                                      'data-zta': 'SelectedArticleBox__TopSection'}).find('div', class_="z-product-price__nowrap").get_text()))
                        discounted_price = float(re.sub(pattern, "", soup.find('span', attrs={
                                                 'data-zta': 'SelectedArticleBox__TopSection'}).find('span', class_="z-product-price__amount--reduced").get_text()))
                        discount_percent = round(
                            (price - float(discounted_price)) / price, 2)
                    else:
                        price = float(re.sub(pattern, "", soup.find('span', attrs={
                                      'data-zta': 'SelectedArticleBox__TopSection'}).find('span', class_="z-product-price__amount").get_text()))
                        discounted_price = None
                        discount_percent = None

                    variants.append(variant)
                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percent)
                    image_urls.append(
                        soup.find('meta', attrs={'property': "og:image"}).get('content'))

                df = pd.DataFrame({
                    "variant": variants,
                    "price": prices,
                    "discounted_price": discounted_prices,
                    "discount_percentage": discount_percentages,
                    "image_urls": image_urls
                })
                df.insert(0, "url", product_url)
                df.insert(0, "description", description)
                df.insert(0, "rating", rating)
                df.insert(0, "name", product_title)
                df.insert(0, "shop", self.SHOP)

                return df
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
