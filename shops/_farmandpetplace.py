from functions.etl import PetProductsETL


class FarmAndPetPlaceETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "FarmAndPetPlace"
        self.BASE_URL = "https://www.farmandpetplace.co.uk"

    def extract_links(self):
        print(f"[{self.SHOP}] Extracting data from categories...")

    def extract_product_info(self):
        print(f"[{self.SHOP}] Extracting data from categories...")

    def transform(self):
        print(f"[{self.SHOP}] Transforming data...")

    def load(self):
        print(f"[{self.SHOP}] Loading data into destination...")
