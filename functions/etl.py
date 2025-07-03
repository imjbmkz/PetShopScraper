import os
import json
import pandas as pd

from abc import ABC, abstractmethod
from sqlalchemy.engine import Engine
from .connection import Connection
from .scraper import scrape_url
from loguru import logger


class PetProductsETL(ABC):
    def __init__(self):
        self.SHOP = ""
        self.BASE_URL = ""
        self.CATEGORIES = []
        self.connection = Connection()

    async def scrape(self, url, selector, headers=None):
        return await scrape_url(url, selector, headers)

    @abstractmethod
    def extract_links(self):
        pass

    @abstractmethod
    def extract_product_info(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    def load(self, data: pd.DataFrame, db_conn: Engine, table_name: str):
        try:
            n = data.shape[0]
            data.to_sql(table_name, db_conn, if_exists="append", index=False)
            logger.info(
                f"Successfully loaded {n} records to the {table_name}.")

        except Exception as e:
            logger.error(e)
            raise e

    def get_links_by_category(self, table_name: str):
        self.connection.execute_query(f"TRUNCATE TABLE {table_name};")

        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        file_path = os.path.join(
            BASE_DIR, 'data', 'categories', f'{self.SHOP.lower()}.json')

        with open(file_path, 'r+') as f:
            d = json.load(f)
            categories = d['data']

            for category in categories:
                df = self.extract_links(category)
                if df is not None:
                    self.load(df, self.connection.engine, table_name)

        sql = self.connection.get_sql_from_file('insert_into_urls.sql')
        self.connection.execute_query(self.connection.engine, sql)
