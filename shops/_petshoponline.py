from functions.etl import PetProductsETL


class PetShopOnlineETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "PetShopOnline"
        self.BASE_URL = "https://pet-shop-online.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = ''
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 2
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 5

    def extract(self, category):
        print(f"[{self.SHOP}] Extracting data from categories...")

    def transform(self):
        print(f"[{self.SHOP}] Transforming data...")
