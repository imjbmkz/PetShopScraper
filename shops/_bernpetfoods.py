import asyncio
import pandas as pd

from functions.etl import PetProductsETL


class BernPetFoodsETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "BernPetFoods"
        self.BASE_URL = "https://www.bernpetfoods.co.uk"

    def extract_links(self, category):
        category_link = f"{self.BASE_URL}{category}"
        urls = []
        page = 1

        while True:
            if page == 1:
                current_url = category_link
            else:
                current_url = f"{category_link}/page/{page}"

            soup = asyncio.run(self.scrape(current_url, '#main-content'))
            if soup:
                product_cards = soup.find_all("div", class_="ftc-product")
                product_links = [product_card.find(
                    "a")["href"] for product_card in product_cards]
                urls.extend(product_links)
                page += 1
                continue

            break

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def extract_product_info(self):
        print(f"[{self.SHOP}] Extracting data from categories...")

    def transform(self):
        print(f"[{self.SHOP}] Transforming data...")
