import logging
from datetime import datetime
import os
import sys
import time
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import asyncio
from pyppeteer import launch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from browser.broswerFunc import close_browser, create_browser
import schedule


#Log File Set
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from log import configure_logging

try:
    logger, _ = configure_logging("EPS.log", "eps")
    print(f"Log File Set!")
except:
    print(f"Logger Setting Error")


# Load environment variables from .env file
load_dotenv()

# URL of the site to scrape
url = 'https://chukul.com/stock-filter'


async def scrape_data():
    logger.info("Starting the web scraping process...")
    # Set up Puppeteer
    browser = await create_browser()
    page = await browser.newPage()
    await page.goto(url)
    logger.info("Opened the URL in the browser.")

    try:
        # Wait for the table headers to load
        await page.waitForSelector('table thead tr', timeout=60000)
        logger.info("Table headers loaded.")
        
        # Wait for the table rows to load
        await page.waitForSelector('table tbody tr', timeout=60000)
        logger.info("Table rows loaded.")

        # Interact with dropdown
        dropdown = await page.waitForXPath('//*[@id="q-app"]/div/div[1]/div/div[2]/main/div[2]/div/div/div[3]/div[2]/label/div/div/div[2]/i', timeout=30000)
        await page.evaluate('(element) => element.scrollIntoView()', dropdown)
        await asyncio.sleep(5)
        await dropdown.click()
        logger.info("Clicked the dropdown.")

        # Wait for network idle
        await asyncio.sleep(1)

        # Select the last item in the dropdown
        dropdown_values = await page.querySelectorAll('.q-virtual-scroll__content .q-item__label')
        await dropdown_values[-1].click()
        logger.info("Selected the last item in the dropdown.")

        # Extract data from the table
        rows = await page.querySelectorAll('table tbody tr')
        extracted_data = []
        for row in rows:
            cells = await row.querySelectorAll('td')
            row_data = []
            for cell in cells:
                cell_text = await page.evaluate('(element) => element.textContent', cell)
                row_data.append(cell_text)
            extracted_data.append(row_data)
        logger.info("Extracted data from the table.")

        # Extract headers from the table
        headers = await page.querySelectorAll('table thead tr th')
        extracted_header = []
        for header in headers:
            header_text = await page.evaluate('(element) => element.textContent', header)
            header_text = header_text.replace('arrow_upward', "")
            extracted_header.append(header_text)
    finally:
        # Close the browser
        await close_browser(browser, page)
        logger.info("Closed the browser.")

    # Filter data
    filtering_columns = ["Symbol", "EPS", "P/E Ratio"]
    filtering_indices = [extracted_header.index(col) for col in filtering_columns]

    final_data = []
    for row in extracted_data:
        final_row = [row[idx] for idx in filtering_indices]
        final_data.append(final_row)

    return final_data

def insert_data_into_database(final_data):
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    # Connect to the database
    try:
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        logger.info("Connection established.")
        cursor = connection.cursor()

        # Retrieve stock symbols and their IDs
        cursor.execute("SELECT id, symbol FROM stock")
        symbols = cursor.fetchall()
        logger.info("Fetched stock symbols and IDs from the database.")

        symbol_dict = {symbol: stock_id for stock_id, symbol in symbols}

        # Insert or update data into the database
        for row in final_data:
            symbol = row[0].strip()
            eps = row[1]
            pe_ratio = row[2]

            # Handle NaN values
            eps = '0.00' if eps == 'NaN' or eps == '' else eps
            pe_ratio = '0.00' if pe_ratio == 'NaN' or pe_ratio == '' else pe_ratio

            current_date = datetime.now()

            stock_id = symbol_dict.get(symbol)
            if stock_id is not None:
                # Check if the stock_id already exists in stock_eps_pe
                cursor.execute("SELECT stock_id FROM stock_eps_pe WHERE stock_id = %s", (stock_id,))
                result = cursor.fetchone()
                
                if result:
                    # If the stock_id exists, update the record
                    sql_update = """
                        UPDATE stock_eps_pe
                        SET EPS = %s, PE_Ratio = %s, updated_at = %s
                        WHERE stock_id = %s
                    """
                    cursor.execute(sql_update, (eps, pe_ratio, current_date, stock_id))
                else:
                    # If the stock_id does not exist, insert a new record
                    sql_insert = """
                        INSERT INTO stock_eps_pe (stock_id, EPS, PE_Ratio, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_insert, (stock_id, eps, pe_ratio, current_date, current_date))
        logger.info("Data Inserted/Updated for EPS and PERatio")
        # Commit the transaction
        connection.commit()
        logger.info("Data committed to the database.")

    except mysql.connector.Error as err:
        logger.error(f"Error: {err}")
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(err)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("Closed the database connection.")

def job():
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(scrape_data())
    insert_data_into_database(data)


def eps():
    while True:
        try:
            logger.info("Initializing schedule_jobs...")
            # Schedule the job to run at 11:20 AM every day
            schedule.every().day.at("11:13").do(job)
            schedule.every().day.at("11:15").do(job)
            schedule.every().day.at("11:20").do(job)
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
    eps()
