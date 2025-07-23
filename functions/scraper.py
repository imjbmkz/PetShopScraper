import random
import asyncio
import nest_asyncio

from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log
)
from loguru import logger
nest_asyncio.apply()

MAX_RETRIES = 5
MAX_WAIT_BETWEEN_REQ = 5
MIN_WAIT_BETWEEN_REQ = 2
REQUEST_TIMEOUT = 60000
PAGE_LOAD_TIMEOUT = 60000


class SkipScrape(Exception):
    """Raised to indicate that scraping should be skipped (e.g. 404)."""
    pass


class ScrapingError(Exception):
    """General scraping error that should trigger retries"""
    pass


class WebScraper:
    def __init__(self):
        self.ua = UserAgent()
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

        self.playwright_instance = None
        self.pages_scraped = 0
        self.restart_browser_every = 10

    def get_headers(self, headers=None) -> Dict[str, str]:
        """Generate realistic browser headers"""

        default_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "User-Agent": self.ua.random,
            "Priority": "u=0, i",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua": '"Not.A/Brand";v="24", "Opera GX";v="118", "Chromium";v="134"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1"
        }

        if headers:
            default_headers.update(headers)

        return default_headers

    async def setup_browser(self) -> None:
        """Initialize browser and context"""
        if self.browser is None:
            self.playwright_instance = await async_playwright().start()

            browser_args = {
                "headless": True,
                "firefox_user_prefs": {
                    # === BIGGEST MEMORY SAVERS ===
                    # Disable images - saves 50-80% memory
                    "permissions.default.image": 2,

                    # === CACHE COMPLETELY DISABLED ===
                    "browser.cache.disk.enable": False,
                    "browser.cache.memory.enable": False,
                    "browser.cache.memory.capacity": 0,
                    "browser.cache.offline.enable": False,
                    "browser.cache.insecure_password_warning": False,

                    # === MEMORY MANAGEMENT ===
                    "memory.free_dirty_pages": True,
                    "memory.ghost_window_timeout_seconds": 1,
                    "dom.memory_reporter.enabled": False,

                    # === MEDIA/AUDIO DISABLED ===
                    "media.volume_scale": "0.0",
                    "media.autoplay.enabled": False,
                    "media.block-autoplay-until-in-foreground": True,
                    "media.suspend-bkgnd-video.enabled": True,

                },
                "args": [
                    "--no-remote",
                    "--disable-gpu",
                ]
            }

            self.browser = await self.playwright_instance.firefox.launch(**browser_args)

        if self.context is None:
            context_options = {
                "locale": "en-US",
                "user_agent": self.ua.random,
                "viewport": {"width": 1920, "height": 1080},
                "java_script_enabled": True,
                "ignore_https_errors": True
            }

            self.context = await self.browser.new_context(**context_options)

            await self.context.route("**/analytics**", lambda route: route.abort())
            await self.context.route("**/ads**", lambda route: route.abort())

    async def _extract_scrape_content(
        self,
        url: str,
        selector: str,
        timeout: int = REQUEST_TIMEOUT,
        wait_until: str = "domcontentloaded",
        simulate_behavior: bool = True,
        headers: Optional[Dict[str, str]] = None,
    ) -> BeautifulSoup:

        page = None
        try:
            await self.setup_browser()

            if not self.context:
                raise ScrapingError("Failed to initialize browser context")

            page = await self.context.new_page()

            page.set_default_timeout(timeout)
            page.set_default_navigation_timeout(PAGE_LOAD_TIMEOUT)

            await page.set_extra_http_headers(self.get_headers(headers))

            logger.info(f"Navigating to: {url}")

            valid_wait_until = {
                "load", "domcontentloaded", "networkidle", "commit"}
            if wait_until not in valid_wait_until:
                logger.warning(
                    f"Invalid wait_until '{wait_until}', defaulting to 'domcontentloaded'")
                wait_until = "domcontentloaded"

            response = await page.goto(url, wait_until=wait_until, timeout=PAGE_LOAD_TIMEOUT)

            if not response:
                raise ScrapingError(f"No response received for {url}")

            if response.status >= 400:
                raise SkipScrape(f"HTTP {response.status} error for {url}")

            logger.info(f"Waiting for selector: {selector}")
            await page.wait_for_selector(selector, timeout=timeout)

            logger.info("Extracting page content...")
            rendered_html = await page.content()

            soup = BeautifulSoup(rendered_html, "html.parser")
            logger.success(f"Successfully extracted content from {url}")

            return soup

        except SkipScrape:
            # Don't retry for SkipScrape exceptions (404, etc.)
            raise
        except asyncio.TimeoutError as e:
            logger.error(
                f"Timeout waiting for selector '{selector}' on {url}: {e}")
            raise ScrapingError(
                f"Timeout waiting for selector '{selector}' on {url}: {e}")
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            raise ScrapingError(f"Error scraping {url}: {str(e)}")

        finally:
            if page:
                try:
                    await page.close()
                    page = None
                except Exception as e:
                    logger.error(f"Error closing page: {e}")

    async def extract_scrape_content(
        self,
        url: str,
        selector: str,
        timeout: int = REQUEST_TIMEOUT,
        wait_until: str = "domcontentloaded",
        simulate_behavior: bool = True,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[BeautifulSoup]:
        try:
            return await retry_extract_scrape_content(
                self, url, selector, timeout, wait_until, simulate_behavior, headers
            )
        except SkipScrape as e:
            logger.warning(f"Skipping scrape: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to scrape after {MAX_RETRIES} attempts: {e}")
            return None

    async def close(self, force: bool = False):
        """Clean up all browser resources."""
        try:
            if self.context:
                await self.context.close()
                self.context = None

            if self.browser:
                logger.info("Closing browser...")
                await self.browser.close()
                self.browser = None

            if self.playwright_instance:
                await self.playwright_instance.stop()
                self.playwright_instance = None

            logger.info("Browser resources fully closed")

        except Exception as e:
            logger.error(f"Error during browser close: {e}")


@retry(
    wait=wait_exponential(
        multiplier=1, min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
    stop=stop_after_attempt(MAX_RETRIES),
    retry=retry_if_exception_type(ScrapingError),
    before_sleep=before_sleep_log(logger, "WARNING"),
    reraise=True,
)
async def retry_extract_scrape_content(scraper, *args, **kwargs):
    return await scraper._extract_scrape_content(*args, **kwargs)


class AsyncWebScraper:
    def __init__(self):
        self.scraper = WebScraper()

    async def __aenter__(self):
        return self.scraper

    async def __aexit__(self, *_):
        await self.scraper.close()


async def scrape_url(url, selector, headers=None, wait_until="domcontentloaded", min_sec=2, max_sec=5) -> Optional[BeautifulSoup]:
    async with AsyncWebScraper() as scraper:
        result = await scraper.extract_scrape_content(url, selector, headers=headers, wait_until=wait_until)
        delay = random.uniform(min_sec, max_sec)
        if delay >= 60:
            minutes = int(delay // 60)
            seconds = delay % 60
            logger.info(f"Sleep for {minutes} min {seconds:.2f} sec")
        else:
            logger.info(f"Sleep for {delay:.2f} sec")

        await asyncio.sleep(delay)
        return result


async def scrape_urls(urls_and_selectors) -> list:
    async with AsyncWebScraper() as scraper:
        results = []
        for url, selector in urls_and_selectors:
            result = await scraper.extract_scrape_content(url, selector)
            results.append(result)

            await asyncio.sleep(random.uniform(2, 5))
        return results
