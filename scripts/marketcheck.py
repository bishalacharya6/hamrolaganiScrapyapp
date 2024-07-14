import asyncio
import logging
import os
import sys
from pyppeteer import launch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from browser.broswerFunc import close_browser, create_browser

# Configure logging
logger = logging.getLogger()


# Main scraping function
async def scrape_market_status(max_retries=3):
    browser = None
    
    for attempt in range(max_retries):
        try:
            browser = await create_browser()
            page = await browser.newPage()
            
            # Set a custom user agent
            await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            
            website = 'https://www.nepalstock.com.np/'
            
            # Increase timeout and add additional wait time
            await page.goto(website, {'waitUntil': 'networkidle0', 'timeout': 90000})
            await asyncio.sleep(5)  # Wait for 5 seconds after page load

            # Wait for the element to be visible
            await page.waitForXPath('/html/body/app-root/div/main/div/app-dashboard/div[1]/div[1]/div/div[1]/div[1]/div[2]/span[2]', {'visible': True, 'timeout': 90000})
            
            # Get the text from the element
            element = await page.xpath('/html/body/app-root/div/main/div/app-dashboard/div[1]/div[1]/div/div[1]/div[1]/div[2]/span[2]')
            market_status_text = await page.evaluate('(element) => element.textContent', element[0])
            market_status_text = market_status_text.lower()
            logger.info(f"Info about market: {market_status_text}")
            logger.info("Market Status Checked")

            # Check if the market is live
            is_live = "live market" in market_status_text

            return is_live

        except Exception as e:
            logger.error(f"An error occurred during status checking (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(5)  # Wait for 5 seconds before retrying

        finally:
            if browser:
                await close_browser(browser, page)


