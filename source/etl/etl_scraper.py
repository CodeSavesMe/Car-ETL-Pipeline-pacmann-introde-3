# source/etl/etl_scraper.py

import os
from loguru import logger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from tqdm.std import tqdm as std_tqdm

from source.etl.utils.etl_selector import (
    LOCATION_INPUT,
    LOCATION,
    ITEM,
    FIRST_ITEM_LINK,
    LOAD_MORE_BUTTON,
)


async def olx_scraper(
    playwright,
    keyword: str,
    html_path: str,
    location: str = "Indonesia",
    headless: bool = False,  # default tetap False biar enak debugging
    goto_timeout_ms: int = 60000,
) -> None:
    """
    Asynchronous web scraper for fetching used car listings.

    Extracts HTML content from a target marketplace, handles pop-ups, sets
    location, performs infinite scrolling to load all items, captures a
    screenshot, and saves HTML locally.

    Parameters
    ----------
    playwright : Playwright object
        The Playwright instance for browser automation.
    keyword : str
        Search keyword to find relevant car listings.
    html_path : str
        Path where the scraped HTML will be saved.
    location : str, optional
        Geographic location filter, default is 'Indonesia'.
    headless : bool, optional
        Whether to run browser in headless mode, default is False.
    goto_timeout_ms : int, optional
        Timeout for page.goto() in milliseconds, default is 60000.
    """

    # --- 1) Build search URL from keyword ---
    keyword_url = keyword.lower().replace(" ", "-")
    url = f"https://www.olx.co.id/mobil-bekas_c198/q-{keyword_url}"

    # logging record
    logger.info(
        f"[Scraper] Start OLX scrape for keyword='{keyword}', url='{url}', location='{location}'"
    )

    # --- 2) Launch browser and page
    browser = await playwright.chromium.launch(headless=headless)
    page = await browser.new_page(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36"
        )
    )

    try:
        # --- 3) Navigate to target page ---
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            logger.info("[Scraper] Page loaded (domcontentloaded)")
        except PlaywrightTimeoutError as e:
            logger.warning(
                f"[Scraper] WARNING: goto timeout after {goto_timeout_ms}ms: {e}. "
                "Continuing with partially loaded content..."
            )

        # --- 4) Pop-up handling ---
        try:
            await page.get_by_role("button", name="Never allow").click(timeout=2000)
            logger.debug("[Scraper] Clicked 'Never allow' notification button")
        except PlaywrightTimeoutError:
            logger.debug("[Scraper] 'Never allow' button not found / timeout, skip")

        # Close generic popup if present
        try:
            await page.click("button[aria-label='Close']", timeout=2000)
            logger.debug("[Scraper] Closed generic popup")
        except PlaywrightTimeoutError:
            logger.debug("[Scraper] Close popup button not found / timeout, skip")

        # --- 5) Set location and load all listings via infinite scroll ---
        logger.info(f"[Scraper] Setting location to '{location}'")
        await page.wait_for_selector(LOCATION_INPUT, timeout=15000)
        await page.fill(LOCATION_INPUT, location)
        await page.wait_for_timeout(500)
        await page.locator(LOCATION, has_text=location).click()
        await page.wait_for_selector(FIRST_ITEM_LINK, timeout=15000)
        logger.info("[Scraper] First item link detected, start loading all listings")

        # --- 6) Infinite scroll to load all listings
        total_listing = 0
        no_new_round = 0
        max_no_new_round = 3

        # Progress bar to show real-time loading feedback
        with std_tqdm(
            desc="Loading items",
            unit=" items",
            dynamic_ncols=True,
            leave=True,
            mininterval=0.5,
        ) as pbar:
            while True:
                # Init Count how many listing items are currently loaded
                current_listing = await page.locator(ITEM).count()

                if current_listing > total_listing:
                    # If new items are found, update totals
                    new_listing = current_listing - total_listing
                    total_listing = current_listing
                    no_new_round = 0
                    pbar.update(new_listing)
                    pbar.set_postfix_str(f"{total_listing} items")
                    logger.debug(f"[Scraper] {total_listing} items loaded so far...")
                else:
                    # No new items detected
                    no_new_round += 1
                    logger.debug(
                        f"[Scraper] No new items this round "
                        f"(round without new={no_new_round}/{max_no_new_round})"
                    )

                try:
                    # Try to click "Load More" button to load more items
                    await page.click(LOAD_MORE_BUTTON, timeout=2000)
                    await page.wait_for_timeout(1500)

                except PlaywrightTimeoutError:
                    # If no button, scroll to bottom to trigger lazy loading
                    await page.evaluate(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    await page.wait_for_timeout(1500)

                # Exit loop after several rounds without new items
                if no_new_round >= max_no_new_round:
                    pbar.set_postfix_str(f"{total_listing} items (complete)")
                    logger.info(
                        f"[Scraper] All listings loaded. Total: {total_listing}"
                    )
                    break

        # --- 7) Save a full-page screenshot ---
        screenshot_name = f"{keyword.replace(' ', '_')}.png"
        screenshot_path = os.path.join("screenshots", screenshot_name)
        os.makedirs("screenshots", exist_ok=True)
        await page.screenshot(path=screenshot_path, full_page=True)
        logger.info(f"[Scraper] Screenshot saved to {screenshot_path}")

        # --- 8) Save HTML ---
        html_content = await page.content()

        html_dir = os.path.dirname(html_path)
        if html_dir:
            os.makedirs(html_dir, exist_ok=True)

        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info(
            f"[Scraper] HTML saved to {html_path} (total items ~ {total_listing})"
        )

    except Exception as e:
        # Handle unexpected errors
        logger.error(f"[Scraper] Unexpected error while scraping '{keyword}': {e}")
        raise

    finally:
        # --- 9) Close browser ---
        await browser.close()
        logger.info("[Scraper] Browser closed")
