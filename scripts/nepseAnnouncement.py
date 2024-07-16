# Import necessary libraries
import os
import random
import sys
import time
import asyncio
import psutil
from pyppeteer import launch
from pyppeteer.errors import TimeoutError, PageError
from dotenv import load_dotenv
from fake_useragent import UserAgent
import logging
import mysql.connector
from datetime import datetime, timedelta, time as dt_time
import aiohttp
import ssl
import schedule

# Add parent directory to system path for custom module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from browser.broswerFunc import close_browser, create_browser
from log import configure_logging

# Load environment variables
load_dotenv()

# Constants
URL = "https://www.nepalstock.com.np/corporatedisclosures"
MAX_RETRIES = 3
RETRY_DELAY = 5

# Configure logging
try:
    logger, _ = configure_logging("announcement.log", "announcement")
except Exception as e:
    print(f'Error Setting logger: {e}')
    sys.exit(1)

# SSL context for API calls
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Function to call API for notification
async def call_notification_api(stock_name):
    """
    Calls the notification API for a given stock name.
    """
    async with aiohttp.ClientSession() as session:
        url = 'https://hamrolagani.com/api/announcement/schedule-notification'
        try:
            async with session.post(url, ssl=ssl_context) as response:
                if response.status == 200:
                    logger.info(f"API call successful for {stock_name} with status {response.status}")
                else:
                    error_message = await response.text()
                    logger.error(f"API call failed for {stock_name} with status {response.status}. Error message: {error_message}")
        except aiohttp.ClientError as e:
            logger.error(f"Error occurred while calling API for {stock_name}: {e}")

# Function to process and store announcement data
async def announcement_data(data_list):
    """
    Processes and stores announcement data in the database.
    """
    current_time = datetime.now().time()

    try:
        # Establish database connection
        db_connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )
        cursor = db_connection.cursor()
        logger.info("Database connection established.")

        # Fetch stock data
        cursor.execute("SELECT id, symbol FROM stock")
        stocks = cursor.fetchall()
        stock_ids = {symbol: stock_id for stock_id, symbol in stocks}
        logger.info("Database stock fetched.")

        current_date = datetime.now().date()
        for row in data_list:
            stock_name = row.get('symbol')
            stock_id = stock_ids.get(stock_name)
            if not stock_id:
                logger.info(f"Stock ID for {stock_name} not found")
                continue

            # Generate random notification time
            minute = lambda: random.randint(30, 59)
            random_minute = minute()
            delta = timedelta(minutes=random_minute)
            new_time = (datetime.combine(datetime.today(), current_time) + delta).time()

            # Process announcement data
            announcement_date = datetime.strptime(row['approved_date'], "%Y-%m-%d %H:%M:%S").date()
            announcement_text = row['announcement']
            should_notify = announcement_date == current_date
            notify_time = new_time if should_notify else None
            
            # Check if announcement already exists
            cursor.execute(
                "SELECT COUNT(*) FROM announcements WHERE stock_id = %s AND date = %s AND announcement = %s",
                (stock_id, announcement_date, announcement_text)
            )
            exists = cursor.fetchone()[0] > 0
            
            # Insert new announcement if it doesn't exist
            if not exists:
                cursor.execute(
                    "INSERT INTO announcements (stock_id, date, announcement, should_notify, notify_time, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (stock_id, announcement_date, announcement_text, should_notify, notify_time, current_date, current_date)
                )

                db_connection.commit()

                # Call notification API
                await call_notification_api(stock_name)
                logger.info("Notification Send")
            

        logger.info("Announcements processed")
    
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
    
    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
        logger.info("Database connection closed.")


# Main scraping function
async def scrape_and_process_announcements():
    """
    Scrapes announcements from the website and processes them.
    """
    ua = UserAgent()
    browser = None
    
    try:
        logger.info("Launching browser")
        browser = await create_browser()        
        page = await browser.newPage()
        
        # Navigation with retry logic
        for attempt in range(MAX_RETRIES):
            try:
                await page.setUserAgent(ua.random)
                logger.info(f"Navigating to the page (attempt {attempt + 1}/{MAX_RETRIES})")
                await page.goto(URL, {'waitUntil': 'networkidle0', 'timeout': 60000})
                logger.info("Page loaded successfully")
                break
            except (TimeoutError, PageError) as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                logger.error(f"Error accessing the page: {e}")
                await asyncio.sleep(RETRY_DELAY)
        
        # Extract headers
        await page.waitForXPath('/html/body/app-root/div/main/div/app-company-news/div[1]/div[3]/table/thead/tr', {'timeout': 30000})
        headers_elements = await page.xpath('/html/body/app-root/div/main/div/app-company-news/div[1]/div[3]/table/thead/tr/th')
        headers_list = [
            (await page.evaluate('(element) => element.innerText.trim().toLowerCase().replace(/\\s+/g, "_")', header))
            for header in headers_elements[1:5]
        ]
        headers_list.append('announcement')
        logger.info(f"Headers: {headers_list}")

        # Extract rows data
        rows = await page.xpath('/html/body/app-root/div/main/div/app-company-news/div[1]/div[3]/table/tbody/tr')
        all_data_list = []

        for index, row in enumerate(rows):
            logger.info(f"Processing row {index + 1}/{len(rows)}")
            cells = await row.xpath('.//td')
            cell_data = {headers_list[i]: await page.evaluate('(element) => element.innerText', cell) for i, cell in enumerate(cells[1:5])}
            
            # Extract announcement
            try:
                button = await row.xpath('.//td[@class="text-left filename"]//a')
                await button[0].click()
                await page.waitForXPath('//*[@id="fileView"]/div/div/div[2]/div[1]/span[2]', {'timeout': 5000})
                announcement_element = await page.xpath('//*[@id="fileView"]/div/div/div[2]/div[1]/span[2]')
                announcement = await page.evaluate('(element) => element.innerText', announcement_element[0])
                cell_data['announcement'] = announcement.strip() if announcement else None
                
                # Close announcement modal
                close_button = await page.xpath('//*[@id="fileView"]/div/div/div[1]/button')
                await close_button[0].click()

            except Exception as e:
                logger.error(f"Error extracting announcement for row {index + 1}: {e}")
                cell_data['announcement'] = None

            # Process and save data
            if not cell_data['announcement']:
                cell_data['announcement'] = cell_data.get('title', '')

            all_data_list.append(cell_data)
            await asyncio.sleep(1)  # Small delay between rows

        logger.info(f"Scraped {len(all_data_list)} announcements")
        await announcement_data(all_data_list)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if browser:
            await close_browser(browser, page)

# Job function to be scheduled
def job():
    logger.info("Starting the scraping process...")
    try:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        loop.run_until_complete(scrape_and_process_announcements())
        
        logger.info("Scraping process completed.")
    
    except Exception as e:
        logger.error(f"An error occurred during the scraping process in Job Function: {e}")

# Main function to run the live indexes
def announcements():
    while True:
        try:
            logger.info("Initializing schedule_jobs for Live Indexes...")
            start_time = dt_time(8, 00)
            end_time = dt_time(17, 30)

            schedule.every(20).minutes.do(job)
            logger.info("Job scheduled successfully.")

            while True:
                current_time = datetime.now().time()
                current_day = datetime.now().weekday()

                if current_day in [6, 0, 1, 2, 3] and start_time <= current_time <= end_time:
                    current_day = datetime.now().strftime("%A")
                    logger.info(f"Today is {current_day}. Running announcement scheduled jobs.")
                    schedule.run_pending()

                time.sleep(30)  # Check every 30 seconds

        except Exception as e:
            logger.error(f"An unexpected error occurred in schedule_jobs: {e}")
            logger.info("Attempting to restart schedule_jobs after a brief pause...")
            time.sleep(60)  # Wait for 60 seconds before trying again
        except KeyboardInterrupt:
            logger.error("Keyboard Interrupted, Exiting the program.")
            break

# Main entry point
if __name__ == "__main__":
    announcements()