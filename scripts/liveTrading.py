import asyncio
from datetime import datetime, time as dt_time
import logging
import os
import sys
import psutil
from pyppeteer import launch
from pyppeteer.errors import TimeoutError
import schedule
import time
from mysql.connector import errorcode
import mysql.connector
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from marketcheck import scrape_market_status
from browser.broswerFunc import close_browser, create_browser
from log import configure_logging

load_dotenv()

# URL for live market data
url = "https://www.nepalstock.com.np/live-market"

# Configure logging
try:
    logger, _ = configure_logging("stockLive.log", "live_stock")
    print(f"Index Log File Set!")
except:
    print(f"Logger Setting Error")

async def live_market():
    browser = None
    page = None

    try:
        logger.info("Creating a new browser instance...")
        browser = await create_browser()
        page = await browser.newPage()

        logger.info("Navigating to the URL...")
        await page.goto(url)
        await page.waitForXPath('/html/body/app-root/div/main/div/app-live-market/div/div/div[5]/table/thead', timeout=60000)

        logger.info("Extracting headers...")
        headers_xpath = '/html/body/app-root/div/main/div/app-live-market/div/div/div[5]/table/thead/tr/th[position()>1]'  # Avoid first column (SN)
        headers_elements = await page.xpath(headers_xpath)

        headers = []
        for element in headers_elements:
            header_text = await page.evaluate('(element) => element.innerText', element)
            header_text = header_text.lower().replace('%', 'percentage').replace(' ', '_')
            headers.append(header_text)
        
        logger.info(f"Headers extracted: {headers}")

        while True:
            logger.info("Extracting rows...")
            try:
                xpath_for_table = "/html/body/app-root/div/main/div/app-live-market/div/div/div[5]/table/tbody"
                # Reduce timeout to detect market closure faster
                await page.waitForXPath(xpath_for_table, timeout=30000)  # 30 seconds timeout
            except TimeoutError:
                logger.warning("No Data Found, Market closed or page expired!")
                break
            except Exception as e:
                logger.error(f"An unexpected error occurred while waiting for table: {e}")
                break

            try:
                rows_xpath = '/html/body/app-root/div/main/div/app-live-market/div/div/div[5]/table/tbody/tr'
                rows_elements = await page.xpath(rows_xpath)
                new_data = []

                for row in rows_elements:
                    row_data = {}
                    cell_elements = await row.xpath('td[position()>1]')  # Avoid first column (SN)
                    for i, cell in enumerate(cell_elements):
                        cell_text = await page.evaluate('(element) => element.innerText', cell)
                        cell_text = cell_text.replace(',', '').strip()
                        row_data[headers[i]] = cell_text
                    new_data.append(row_data)
                logger.info(f"Extracted {len(new_data)} rows of data.")
                print(new_data)
                insert_data_into_database(new_data)

                # Add a short delay to avoid overwhelming the server
                time.sleep(20)

            except Exception as e:
                logger.error(f"Error extracting data: {e}")

    except TimeoutError:
        logger.error("Timeout while loading the page or finding the element.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if browser:
            logger.info("Closing the browser...")
            await close_browser(browser, page)



def insert_data_into_database(final_data):

    try:
        # Connect to the database
        try:
            connection = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE')
            )
            logger.info("Connection established.")
            cursor = connection.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Database does not exist")
            else:
                logger.error(f"Database Connection Error: {err}")
            raise

        # Prepare the insert statement with placeholders
        sql = """
            INSERT INTO live_trading (
                stock_id, LTP, LTV, point_change, percentage_change, open,
                high, low, avg_trading_price, volume, previous_closing, 
                created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Retrieve stock symbols and their IDs
        cursor.execute("SELECT id, symbol FROM stock")
        symbols = cursor.fetchall()

        stock_dict = {symbol: stock_id for stock_id, symbol in symbols}
        logger.info("Stock dictionary created.")
        
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert data into the database
        for row in final_data:
            stock_id = stock_dict.get(row['symbol'])
            if stock_id is not None:
                try:
                    values = (
                        stock_id,
                        row['ltp'],
                        row['ltv'],
                        row['point_change'],
                        row['percentage_change'],
                        row['open_price'],
                        row['high_price'],
                        row['low_price'],
                        row['avg_traded_price'],
                        row['volume'],
                        row['previous_closing'],
                        current_time,
                        current_time
                    )
                    cursor.execute(sql, values)
                except KeyError as e:
                    logger.error(f"Missing key in data row: {e}. Row data: {row}")
                except Exception as e:
                    logger.error(f"Error inserting row: {e}. Row data: {row}")
            else:
                logger.warning(f"No matching stock_id found for symbol {row['symbol']}")

        # Commit the transaction
        connection.commit()
        logger.info("Data committed to the database.")

    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(str(err))
    except Exception as e:
        logger.error(f"An unexpected error occurred during database operation: {e}")
    finally:
        if cursor:
            try:
                cursor.close()
                logger.info("Cursor closed.")
            except Exception as e:
                logger.error(f"Error closing cursor: {e}")
        if connection and connection.is_connected():
            try:
                connection.close()
                logger.info("Database connection closed.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
def job():
    """
    Job function to be scheduled. It checks the market status and runs the scraper if conditions are met.
    """
    current_time = datetime.now().time()
    market_close_time = dt_time(15, 5)  # 3:05 PM
    is_live = asyncio.run(scrape_market_status())
    
    # Log current market status and time
    logger.info(f"Market live status: {'True' if is_live else 'False'}, Current time: {current_time}")

    # Run the scraper if the market is live or if it's between 3:01 PM and 3:05 PM
    if is_live or (dt_time(15, 1) <= current_time < market_close_time):
        logger.info("Starting the scraping process...")
        try:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            loop.run_until_complete(live_market())
        
        except Exception as e:
            logger.error(f"An error occurred during the scraping process in Job Function: {e}")


def live_stock():
    """
    Main function to schedule and run the live stock data collection job.
    """
    while True:
        try:
            logger.info("Initializing schedule_jobs for Live Indexes...")
            start_time = dt_time(11, 00)
            end_time = dt_time(15, 5)

            schedule.every(1).minutes.do(job)
            logger.info("Job scheduled successfully.")

            while True:
                current_time = datetime.now().time()
                current_day = datetime.now().weekday()

                # Run jobs on specified days (Sunday to Thursday) and within the specified time range
                if current_day in [6, 0, 1, 2, 3] and start_time <= current_time <= end_time:
                    logger.info(f"Current time is {current_time}. Running scheduled jobs.")
                    schedule.run_pending()

                time.sleep(10)  # Check every 10 seconds

        except Exception as e:
            logger.error(f"An unexpected error occurred in schedule_jobs: {e}")
            logger.info("Attempting to restart schedule_jobs after a brief pause...")
            time.sleep(60) # Wait for 60 seconds before trying again
        except KeyboardInterrupt:
            logger.error("Keyboard Interrupted, Exiting the program.")
            break  

if __name__ == "__main__":
    live_stock()