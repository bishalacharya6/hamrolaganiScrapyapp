import asyncio
from datetime import datetime
import os
import sys
import time
from pyppeteer.errors import TimeoutError
import mysql.connector
from dotenv import load_dotenv
import pytz
import schedule

# Set up logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from browser.broswerFunc import close_browser, create_browser

from log import configure_logging


try:
    logger, _ = configure_logging("Floorsheet.log", "floorsheet")
    logger.info(f"Log File Set!")
except Exception as e:
    logger.error(f"Logger Setting Error: {e}")

load_dotenv()


url = "https://www.nepalstock.com.np/floor-sheet"

async def scrapy_extraction():
    browser = None
    page = None
    all_data = []
    try:
        try:
            logger.info("Creating browser instance...")
            browser = await create_browser()
            page = await browser.newPage()
        except Exception as e:
            logger.error(f"An error occurred while creating browser instance: {e}")
            return all_data

        try:
            logger.info(f"Navigating to {url}...")
            await page.goto(url)
            await asyncio.sleep(2)
        except TimeoutError:
            logger.error("Timeout while navigating to the URL.")
            return all_data
        except Exception as e:
            logger.error(f"An error occurred while navigating to the URL: {e}")
            return all_data

        try:
            logger.info("Extracting headers...")
            headers_xpath = '/html/body/app-root/div/main/div/app-floor-sheet/div/div[4]/table/thead/tr/th'
            headers_elements = await page.xpath(headers_xpath)
            await asyncio.sleep(2)
        except TimeoutError:
            logger.error("Timeout while extracting headers.")
            return all_data
        except Exception as e:
            logger.error(f"An error occurred while extracting headers: {e}")
            return all_data

        headers = []
        try:
            for i, element in enumerate(headers_elements):
                if i == 0:  # Skip the first column (serial number)
                    continue
                header_text = await page.evaluate('(element) => element.innerText', element)
                header_text = header_text.strip().lower().replace(' ', '_').replace('(rs)', '')
                header_text = header_text.replace('contract_no.', 'transaction_no')
                header_text = header_text.replace('stock_symbol', 'symbol')
                header_text = header_text.replace('buyer.', 'buyer_broker_id')
                header_text = header_text.replace('seller.', 'sell_broker_id')
                header_text = header_text.replace('quantity.', 'share_quantity')
                header_text = header_text.replace('rate_', 'rate')
                header_text = header_text.replace('amount_', 'amount')
                headers.append(header_text)
            headers.append('date')  # Add date column to headers
            logger.info(f"Headers extracted: {headers}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"An error occurred while processing headers: {e}")
            return all_data

        try:
            logger.info("Selecting 500 entries per page...")
            select_xpath = '/html/body/app-root/div/main/div/app-floor-sheet/div/div[3]/div/div[5]/div/select'
            await page.waitForXPath(select_xpath)
            select_element = await page.xpath(select_xpath)
            await page.evaluate('(element) => { element.value = "500"; element.dispatchEvent(new Event("change")); }', select_element[0])
            await asyncio.sleep(2)  # Wait for the change to take effect
        except TimeoutError:
            logger.error("Timeout while selecting the number of entries per page.")
            return all_data
        except Exception as e:
            logger.error(f"An error occurred while selecting the number of entries per page: {e}")
            return all_data

        try:
            logger.info("Clicking the filter button...")
            filter_button_xpath = '/html/body/app-root/div/main/div/app-floor-sheet/div/div[3]/div/div[6]/button[1]'
            filter_button = await page.waitForXPath(filter_button_xpath)
            await page.evaluate('(element) => element.click()', filter_button)
            await asyncio.sleep(2)
        except TimeoutError:
            logger.error("Timeout while clicking the filter button.")
            return all_data
        except Exception as e:
            logger.error(f"An error occurred while clicking the filter button: {e}")
            return all_data

        page_num = 1
        current_date = datetime.now().date()

        try:
            while True:
                logger.info(f"Extracting data from page {page_num}...")
                rows_xpath = '/html/body/app-root/div/main/div/app-floor-sheet/div/div[4]/table/tbody/tr'
                rows_elements = await page.xpath(rows_xpath)

                for row in rows_elements:
                    cells = await row.xpath('./td')
                    row_data = []
                    for i, cell in enumerate(cells):
                        if i == 0:  # Skip the first column (serial number)
                            continue
                        cell_text = await page.evaluate('(element) => element.innerText', cell)
                        cell_text = cell_text.replace(',', '').strip() 
                        row_data.append(cell_text)
                    row_data.append(str(current_date))  
                    all_data.append(row_data)

                logger.info(f"Extracted {len(rows_elements)} rows from page {page_num}")

                # Check if next button is disabled
                next_button = await page.xpath('/html/body/app-root/div/main/div/app-floor-sheet/div/div[5]/div[2]/pagination-controls/pagination-template/ul/li[10]')
                if next_button:
                    class_property = await next_button[0].getProperty('className')
                    await page.waitFor(2000)
                    class_value = await class_property.jsonValue()
                    print(class_value)
                    if "disabled" in class_value:
                        logger.info("Reached the last page. Stopping extraction.")
                        break
                    
                    logger.info("Clicking next page button...")
                    await next_button[0].click()
                    await asyncio.sleep(2)
                    page_num += 1
                else:
                    logger.info("Next button not found. Stopping extraction.")
                    break

            logger.info(f"Total extracted rows: {len(all_data)}")
        except TimeoutError:
            logger.error("Timeout while extracting data.")
            return all_data
        except Exception as e:
            logger.error(f"An error occurred while extracting data: {e}")
            return all_data

        return all_data
    finally:
        if browser:
            logger.info("Closing the browser...")
            await close_browser(browser, page)

def insert_data_to_database(final_data):
    logger.info("Connecting to the database...")
    
    connection = None
    cursor = None
    
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )
        logger.info("Database connection established.")
        
        cursor = connection.cursor(dictionary=True)
        current_datetime = datetime.now(pytz.timezone('Asia/Kathmandu')).strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('SELECT * FROM stock')
        stock_rows = cursor.fetchall()

        logger.info("Inserting/updating Floorsheet Data")
        if stock_rows:
            for row in final_data:
                stock = next((stock for stock in stock_rows if stock['symbol'] == row[1]), None)
                
                if stock:
                    stock_id = stock['id']
                    buyer_broker_id = int(row[2])
                    sell_broker_id = int(row[3])
                    share_quantity = float(row[4].replace(",", ""))
                    rate = float(row[5].replace(",", ""))
                    amount = float(row[6].replace(",", ""))
                    traded_date = row[7]
                    
                    sql_live_floorsheet = """
                        INSERT INTO floorsheet (stock_id, transaction_no, buyer_broker_id, sell_broker_id, share_quantity, rate, amount, date, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    try:
                        cursor.execute(sql_live_floorsheet, (
                            stock_id,
                            row[0],
                            buyer_broker_id,
                            sell_broker_id,
                            share_quantity,
                            rate,
                            amount,
                            traded_date,
                            current_datetime,
                            current_datetime
                        ))
                    except Exception as error:
                        logger.error(f'Error during Floorsheet Data Insertion: {error}')
                else:
                    logger.warning(f"Skipping row due to missing stock: {row}")

            connection.commit()  # Ensure the transaction is committed
            logger.info("Floorsheet Data Inserted.")
        else:
            logger.warning("No stock or broker data found")

    except Exception as e:
        logger.error(f"Connection to database failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()
            logger.info("Database connection closed.")


def job():
    try:
        logger.info("Running Job func")
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(scrapy_extraction())
        insert_data_to_database(data)
    except:
        logger.error("Error While running Job Function")

def dailyFloorsheet():
    while True:
        try:
            logger.info("Initializing schedule_jobs...")
            # Schedule the job to run at 11:20 AM every day
            schedule.every().day.at("15:30").do(job)
            logger.info("Job scheduled successfully.")

            while True:
                current_day = datetime.now().weekday()

                if current_day in [6, 0, 1, 2, 3]:  # Sunday to Thursday
                    schedule.run_pending()
                time.sleep(5) 

        except Exception as e:
            logger.error(f"An unexpected error occurred in schedule_jobs: {e}")
            logger.error(f"Restarting the program in 1 min.")
            time.sleep(60)
        except KeyboardInterrupt:
            sys.exit('Exiting! Interrupted by User')
            

if __name__ == "__main__":
    job()

