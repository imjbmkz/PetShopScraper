import asyncio
import pandas as pd
import json
from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger


class JollyesETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "Jollyes"
        self.BASE_URL = "https://www.jollyes.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = ''
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 2
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 5

    def extract(self, category):
        category_link = f"{self.BASE_URL}/{category}.html"
        soup = self.extract_from_url("GET", category_link)

        if soup:
            subcategory_links = []
            ul_tags = soup.select("ul[class='second-category']")
            for ul_tag in ul_tags:
                links = ul_tag.select("a")
                for link in links:
                    subcategory_links.append(link["href"])
            urls = []
            start_index = 1
            for subcategory in subcategory_links:
                n = start_index
                while True:
                    url = f"{self.BASE_URL}{subcategory}?page={n}&perPage=100"
                    soup = self.extract_from_url("GET", url)
                    if soup:
                        product_tiles = soup.select(
                            "div[class*='product-tile']")
                        for product_tile in product_tiles:
                            urls.append(self.BASE_URL +
                                        product_tile.select_one("a")["href"])

                        progress = soup.select_one(
                            "div[class*='progress-row w-100']")
                        if progress:
                            n += 1
                            continue
                        break
                    n += 1
            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)
            return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            data = json.loads(soup.select_one(
                "section[class*='lazy-review-section']").select_one("script[type*='application']").text)
            product_title = data["name"]
            description = data["description"]

            if "aggregateRating" in data.keys():
                rating = data["aggregateRating"]["ratingCount"]
            else:
                rating = None

            product_url = url.replace(self.BASE_URL, "")
            price = float(data["offers"]["price"])

            df = pd.DataFrame(
                {
                    "shop": "Jollyes",
                    "name": product_title,
                    "rating": rating,
                    "description": description,
                    "url": product_url,
                    "price": price,
                    "image_urls": ', '.join(data['image']),
                    "variant": None,
                    "discounted_price": None,
                    "discount_percentage": None
                }, index=[0]
            )

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
