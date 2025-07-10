import asyncio
import math
import pandas as pd
from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger


class ViovetETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "Viovet"
        self.BASE_URL = "https://www.viovet.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '#family_page'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 2
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 5

    def extract(self, category):
        current_url = f"{self.BASE_URL}{category}"
        urls = []

        additional_headers = {
            "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8,zh-CN;q=0.7,zh;q=0.6"
        }

        soup = asyncio.run(self.scrape(
            current_url, '#full_search_form', headers=additional_headers))

        pagination_length = 0
        product_number = int(soup.find('div', class_="pagination").find_all(
            'a')[-2].get_text(strip=True))
        if (product_number <= 36):
            page_url = f"{current_url}?page=1"

            page_pagination_source = asyncio.run(
                self.scrape(page_url, '.family-listing-grid'))
            product_list = page_pagination_source.select(
                'a[class*="ab_var_one grid-box _one-whole _no-padding _no-margin"][itemprop="url"]')

            for product in product_list:
                title_tag = product.find('h2', itemprop="name")
                if title_tag:
                    urls.append(self.BASE_URL + product.get('href'))

        else:
            pagination_length = math.ceil(product_number / 36)
            for i in range(1, pagination_length + 1):
                page_url = f"{current_url}?page={i}"
                page_pagination_source = asyncio.run(
                    self.scrape(page_url, '.family-listing-grid'))
                product_list = page_pagination_source.select(
                    'a[class*="ab_var_one grid-box _one-whole _no-padding _no-margin"][itemprop="url"]')

                for product in product_list:
                    title_tag = product.find('h2', itemprop="name")
                    if title_tag:
                        urls.append(self.BASE_URL + product.get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.select_one(
                'h1[id="product_family_heading"]').get_text()
            product_url = url.replace(self.BASE_URL, "")

            product_description_wrapper = soup.select_one(
                'div[itemprop="description"]').find('div').find_all('p')
            description_text = [para.get_text()
                                for para in product_description_wrapper]
            product_description = ' '.join(description_text)

            rating_average = ''
            if (soup.find('span', itemprop="ratingValue") == None):
                rating_average = '0/5'
            else:
                rating_average = soup.select_one(
                    'span[itemprop="ratingValue"]').get_text() + '/5'

            variants_wrapper = soup.find_all('li', 'product-select-item')

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            for variant in variants_wrapper:
                name_span = variant.find('span', 'name')
                clearance_label = name_span.find(
                    'span', 'clearance_product_label')
                if clearance_label:
                    clearance_label.extract()

                name_variant = name_span.get_text(strip=True)
                price_variant = float(variant.find(
                    'span', 'price').get_text(strip=True).replace("£", ""))

                variants.append(name_variant)
                prices.append(price_variant)
                discounted_prices.append(None)
                discount_percentages.append(None)
                image_urls.append(', '.join([
                    'https' + (src if src else data_src)
                    for img in soup.find_all('div', class_="swiper-slide")
                    if (img_tag := img.find('img')) and ((src := img_tag.get('src')) or (data_src := img_tag.get('data-src')))
                ]))

            df = pd.DataFrame({"variant": variants, "price": prices,
                               "discounted_price": discounted_prices, "discount_percentage": discount_percentages, "image_urls": image_urls})
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", rating_average)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
