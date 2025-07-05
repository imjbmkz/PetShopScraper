import math
import asyncio
import pandas as pd

from functions.etl import PetProductsETL


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
        soup = asyncio.run(self.scrape(url, ''))
        n_product = [int(word) for word in soup.find(
            'p', class_="woocommerce-result-count").get_text().split() if word.isdigit()][0]
        pagination_length = math.ceil(n_product / 24)

        urls = []
        if pagination_length == 1:
            urls.extend([self.BASE_URL + product.find('a').get('href') for product in soup.find(
                'div', class_="shop-filters-area").find_all('div', class_="product")])
        else:
            for i in range(1, pagination_length + 1):
                base = url.split("page-")[0]
                new_url = f"{base}page-{i}.html"
                soup_pagination = asyncio.run(self.scrape(new_url, ''))

                urls.extend([self.BASE_URL + product.find('a').get('href') for product in soup_pagination.find(
                    'div', class_="shop-filters-area").find_all('div', class_="product")])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self):
        print(f"[{self.SHOP}] Transforming data...")
