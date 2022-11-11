import logging
from dataclasses import dataclass

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

logger = logging.getLogger(__name__)


@dataclass
class BrowserParams:
    user_agent: str


async def get_browser_params() -> BrowserParams:
    browser_params = BrowserParams(user_agent="")

    async with async_playwright() as p:
        device = p.devices["iPhone 13 Pro Max"]
        browser_params.user_agent = device["user_agent"]

        browser: Browser = await p.webkit.launch(headless=False)
        context: BrowserContext = await browser.new_context(**device)
        page: Page = await context.new_page()

        await page.goto("https://m.tiktok.com/")

        logger.debug(f"Opened a page titled {await page.title()}")

        await context.close()
        await browser.close()

        logger.info(f"Obtained the following browser parameters:\n{browser_params}")

        return browser_params
