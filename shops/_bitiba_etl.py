import re
import json
import pandas as pd
import asyncio

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
        category_link = f"{self.BASE_URL}{category}"
        urls = []

        soup = asyncio.run(self.scrape(
            category_link, '#page-content', min_sec=self.MIN_SEC_SLEEP_PRODUCT_INFO, max_sec=self.MAX_SEC_SLEEP_PRODUCT_INFO))
        if soup:
            product_group_cards = soup.select(
                "a[class*='ProductGroupCard_productGroupLink']")
            product_group_links = [self.BASE_URL + card["href"]
                                   for card in product_group_cards]

            for product_group_link in product_group_links:
                current_url = product_group_link
                while True:
                    soup = asyncio.run(self.scrape(
                        current_url, '#__next', min_sec=self.MIN_SEC_SLEEP_PRODUCT_INFO, max_sec=self.MAX_SEC_SLEEP_PRODUCT_INFO))
                    if soup:
                        products_list = soup.select(
                            "script[type*='application/ld+json']")
                        if products_list:

                            product_list_json = json.loads(
                                products_list[-1].text)
                            if "itemListElement" in product_list_json.keys():
                                product_urls = pd.DataFrame(json.loads(
                                    products_list[-1].text)["itemListElement"])["url"].to_list()
                                urls.extend(product_urls)

                                # Repeat the process if there are new pages
                                next_page_a = soup.find(
                                    "a", attrs={"data-zta": "paginationNext"})
                                if next_page_a:
                                    current_url = next_page_a["href"]
                                    continue

                    break

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
