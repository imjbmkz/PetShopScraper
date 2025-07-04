from functions.etl import PetProductsETL


class ThePetExpressETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "ThePetExpress"
        self.BASE_URL = "https://www.thepetexpress.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = ''

    def extract(self):
        print(f"[{self.SHOP}] Extracting data from categories...")

    def transform(self):
        print(f"[{self.SHOP}] Transforming data...")
