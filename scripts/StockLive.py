import asyncio
import os
import sys
import time
import pytz
from datetime import datetime, time as dt_time
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import schedule

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from browser.broswerFunc import close_browser, create_browser
from log import configure_logging

from scripts.marketcheck import scrape_market_status


try:
    logger, _ = configure_logging("stockLive.log", "live_stock")
    logger.info(f"Log File Set!")
except:
    logger.error(f"Logger Setting Error")


# Load environment variables
load_dotenv()


# URL of the site to scrape
url = 'https://www.sharesansar.com/live-trading'

async def insert_or_update_data_into_database(data):
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host = os.getenv('MYSQL_HOST'),
            user = os.getenv('MYSQL_USER'),
            password = os.getenv('MYSQL_PASSWORD'),
            name = os.getenv('MYSQL_DATABASE')
        )
        cursor = connection.cursor(dictionary=True)

        current_datetime = datetime.now(pytz.timezone('Asia/Kathmandu')).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Starting data update at: {current_datetime}")

        try:
            cursor.execute('SELECT * FROM stock')
            stock_rows = cursor.fetchall()
            logger.info(f"Fetched {len(stock_rows)} stocks from the database")
        except Exception as e:
            logger.info(f"Error fetching stocks: {e}")
            return

        # Data Insertion
        logger.info("Preparing to insert data into Daily Stock Prices")
        time1 = time.time()
        logger.info(f"Time Start: {time1}")
        try:
            bulk_insert_daily = []
            bulk_live_operations = []
            stock_exists_dict = {}

            for row in data:
                try:
                    stock = next((stock for stock in stock_rows if stock['symbol'] == row['symbol']), None)
                    if stock:
                        stock_id = stock['id']
                        bulk_insert_daily.append((
                            stock_id, row['ltp'], row['pointchange'], row['percentagechange'],
                            row['open'], row['high'], row['low'], row['volume'],
                            row['prev.close'], current_datetime, current_datetime
                        ))

                        # Check if the stock exists in live_stock_prices
                        if stock_id not in stock_exists_dict:
                            check_stock_query = "SELECT COUNT(*) FROM live_stock_prices WHERE stock_id = %s"
                            cursor.execute(check_stock_query, (stock_id,))
                            stock_exists_dict[stock_id] = cursor.fetchone()['COUNT(*)'] > 0

                        bulk_live_operations.append((
                            row['ltp'], row['pointchange'], row['percentagechange'],
                            row['open'], row['high'], row['low'], row['volume'],
                            row['prev.close'], current_datetime, current_datetime, stock_id
                        ))
                except Exception as e:
                    logger.error(f"Error processing row: {e}")
                    logger.error(f"Problematic row data: {row}")

            # Insert into daily_stock_prices
            if bulk_insert_daily:
                try:
                    sql_daily_stock_prices = """
                        INSERT INTO daily_stock_prices (
                            stock_id, last_trading_price, point_changes, percentage_change, 
                            open_price, high, low, volume, previous_close_price, created_at, updated_at
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(sql_daily_stock_prices, bulk_insert_daily)
                    logger.info("Data Inserted Successfully into Daily Stock Prices")
                except Exception as e:
                    logger.error(f"Error while inserting bulk data into database: {e}")

            # Prepare update and insert queries for live_stock_prices
            bulk_update = [op for op in bulk_live_operations if stock_exists_dict[op[-1]]]
            bulk_insert = [op for op in bulk_live_operations if not stock_exists_dict[op[-1]]]

            # Update existing records in live_stock_prices
            if bulk_update:
                try:
                    update_query = """
                        UPDATE live_stock_prices
                        SET
                            last_trading_price = %s,
                            point_changes = %s,
                            percentage_change = %s,
                            open_price = %s,
                            high = %s,
                            low = %s,
                            volume = %s,
                            previous_close_price = %s,
                            created_at = %s,
                            updated_at = %s
                        WHERE stock_id = %s
                    """
                    cursor.executemany(update_query, bulk_update)
                    logger.info("Bulk update completed for live_stock_prices")
                except Exception as e:
                    logger.error(f"Error during bulk update: {e}")

            # Insert new records into live_stock_prices
            if bulk_insert:
                try:
                    insert_query = """
                        INSERT INTO live_stock_prices (
                            stock_id,
                            last_trading_price,
                            point_changes,
                            percentage_change,
                            open_price,
                            high,
                            low,
                            volume,
                            previous_close_price,
                            created_at,
                            updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, bulk_insert)
                    logger.info("Bulk insert completed for live_stock_prices")
                except Exception as e:
                    logger.error(f"Error during bulk insert: {e}")

            # Commit the transaction
            try:
                connection.commit()
                logger.info(f"Data inserted or updated in the database at: {current_datetime}")
            except Exception as e:
                logger.error(f"Error committing changes: {e}")

        except Exception as e:
            logger.error(f"Inserting daily stock prices not working: {e}")
        time2 = time.time()
        logger.info(f"Time ended: {time2}")
        logger.info(f"Total Time Taken: {time2-time1}")

        logger.info("Data processing for Live Stock Prices completed")

    except Error as error:
        logger.info(f"Database connection failed: {error}")

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            logger.info("Database connection closed.\n")


async def scrape_website():
    try:
        logger.info("Setting up the Pyppeteer browser...")

        browser = await create_browser()
        page = await browser.newPage()
        await page.goto(url)
        logger.info("Website opened.")

        await page.waitForSelector('#headFixed thead tr th', {'timeout': 60000})
        logger.info("Table headers loaded.")

        headers = await page.querySelectorAll('#headFixed thead tr th')
        header_names = [
            await page.evaluate('(element) => element.textContent', header)
            for header in headers
        ]
        header_names = [
            name.strip().lower().replace(' ', '').replace('/', '').replace('%', 'percentage') 
            for name in header_names
        ]

        logger.info(f"Headers of the table: {header_names}")

        target_time = dt_time(15, 2)
        while True:
            rows = await page.querySelectorAll('#headFixed tbody tr')
            extracted_data = []
            for row in rows:
                cells = await row.querySelectorAll('td')
                row_data = {}
                for idx, cell in enumerate(cells):
                    cell_text = await page.evaluate('(element) => element.textContent', cell)
                    row_data[header_names[idx]] = cell_text.replace(',', '').strip()
                extracted_data.append(row_data)
            logger.info(f"Rows Extracted.\n")
            await insert_or_update_data_into_database(extracted_data)
            
            #Reloads the page
            await page.reload()

            time.sleep(10)
            # Break Statement
            if datetime.now().time() >= target_time:
                break 


    except Exception as e:
        logger.info(f"Error during web scraping: {e}")

    finally:
        await close_browser(browser, page)
        logger.info("Closed the browser.")
  

def job():

    # Log current market status and time
    is_live = True
    for _ in range(10):
        is_live = asyncio.run(scrape_market_status())
        logger.info(f"Market live status: {'True' if is_live else 'False'}")
        if is_live:
            break
        time.sleep(30)

    # Run the scraper if the market is live or if it's between 3:01 PM and 3:05 PM
    if is_live:
        logger.info("Starting the scraping process...")
        try:
            try:
                asyncio.run(scrape_website())
                logger.info("Scraping process completed.")
            except RuntimeError:
                logger.error("Error While running scrap_website Function")
            
        except Exception as e:
            logger.error(f"An error occurred during the scraping process in Job Function: {e}")


def LiveStockPrices():
    while True:
        try:
            logger.info("Initializing schedule_jobs...")
            schedule.every().day.at("14:10").do(job)
            logger.info("Job scheduled successfully.")

            while True:
                current_time = datetime.now().time()
                current_day = datetime.now().weekday()

                if current_day in [6, 0, 1, 2, 3, 4]:
                    logger.info(f"Current time is {current_time}. Running scheduled jobs.")
                    schedule.run_pending()

                time.sleep(20)  # Check every 10 seconds

        except Exception as e:
            logger.error(f"An unexpected error occurred in schedule_jobs: {e}")
            logger.info("Attempting to restart schedule_jobs after a brief pause...")
            time.sleep(60) # Wait for 60 seconds before trying again
        except KeyboardInterrupt as k:
            logger.error(f"Keyboard Interrupted, Exiting the program.")
            break  


if __name__ == "__main__":
    LiveStockPrices()

