from functions.etl import PetProductsETL
from bs4 import BeautifulSoup


class PetSupermarketETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "PetSupermarket"
        self.BASE_URL = "https://www.pet-supermarket.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = ''
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 2
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 5

    def extract(self, category):
        print(f"[{self.SHOP}] Extracting data from categories...")

    def transform(self, soup: BeautifulSoup, url: str):
        print(f"[{self.SHOP}] Transforming data...")
