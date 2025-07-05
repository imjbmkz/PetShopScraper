import re
import math
import asyncio
import pandas as pd
from functions.etl import PetProductsETL


class DirectVetETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "DirectVet"
        self.BASE_URL = "https://www.direct-vet.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = ''
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 2
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 5

    def extract(self, category):
        current_url = f"{self.BASE_URL}/{category}"
        urls = []

        soup = asyncio.run(self.scrape(current_url, '#center_column'))

        if (soup.find('small', class_="heading-counter").get_text() == 'There are no products in this category.'):
            return None

        product_count = int(re.sub(r"There is |There are | products\.| product.", "",
                                   soup.find('small', class_="heading-counter").get_text()))
        pagination_page_num = math.ceil(product_count / 12)

        for i in range(1, pagination_page_num + 1):
            page_url = f"{current_url}?selected_filters=page-{i}"

            page_pagination_source = asyncio.run(
                self.scrape(page_url, '#center_column'))

            for link in page_pagination_source.find_all('a', class_="product_img_link"):
                urls.append(link.get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self):
        print(f"[{self.SHOP}] Transforming data...")
