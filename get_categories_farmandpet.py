import requests
import json
import re
import asyncio
import pandas as pd

from functions.scraper import scrape_url

base_url = 'https://www.farmandpetplace.co.uk'

base_category_url = [
    '/shop/products/pet/dog/',
    '/shop/products/pet/cat/',
    '/shop/products/pet/small-pet/',
    '/shop/products/pet/bird/',
    '/shop/products/pet/reptile/',
    '/shop/products/pet/fish/',
    '/shop/products/pet/equine/',
    '/shop/products/pet/poultry/',
    '/shop/products/pet/farm/',
]

category_url_scrape = []


async def recursive_scrape(category_urls):
    next_layer_urls = []

    for category_url in category_urls:
        soup = await scrape_url(base_url + category_url, '.content-page')

        scrape_links = [
            link.find('a').get('href')
            for link in soup.find_all('div', class_="content-product")
            if link.find('a') and 'page-1.html' in link.find('a').get('href', '')
        ]
        category_url_scrape.extend(scrape_links)

        deeper_links = [
            link.find('a').get('href')
            for link in soup.find_all('div', class_="content-product")
            if link.find('a') and 'page-1.html' not in link.find('a').get('href', '')
        ]
        next_layer_urls.extend(deeper_links)

    if next_layer_urls:
        await recursive_scrape(list(set(next_layer_urls)))

asyncio.run(recursive_scrape(base_category_url))

output = {"data": list(set(category_url_scrape))}

with open(f"data/categories/farmandpetplace.json", "w", encoding="utf-8") as f:
    json.dump(output, f, indent=4)
