import pandas as pd
import asyncio
from functions.etl import PetProductsETL


class AsdaETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "ASDAGroceries"
        self.BASE_URL = "https://groceries.asda.com"

    def extract_links(self, category):
        category_link = f"{self.BASE_URL}{category}"
        urls = []

        soup = asyncio.run(self.scrape(category_link, '.layout__main'))

        if soup.find('div', class_="co-pagination"):
            n_pages = int(
                soup.find('div', class_="co-pagination__max-page").text)

            for p in range(1, n_pages):
                soup_page_pagination = asyncio.run(
                    self.scrape(f"{category_link}?page={p}", '#main-content'))
                for product_container in soup_page_pagination.find_all('ul', class_="co-product-list__main-cntr"):
                    for product_list in product_container.find_all('li'):
                        if product_list.find('a'):
                            urls.append(self.BASE_URL +
                                        product_list.find('a').get('href'))

        else:
            for product_container in soup.find_all('ul', class_="co-product-list__main-cntr"):
                for product_list in product_container.find_all('li'):
                    if product_list.find('a'):
                        urls.append(self.BASE_URL +
                                    product_list.find('a').get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def extract_product_info(self):
        print(f"[{self.SHOP}] Extracting data from categories...")

    def transform(self):
        print(f"[{self.SHOP}] Transforming data...")

    def load(self):
        print(f"[{self.SHOP}] Loading data into destination...")
