from datetime import datetime
import os
import sys
import time
from pyppeteer.errors import TimeoutError
import pytz
import mysql.connector
from dotenv import load_dotenv
import asyncio
import os
import schedule
from concurrent.futures import ProcessPoolExecutor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from browser.broswerFunc import close_browser, create_browser


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from log import configure_logging


load_dotenv()

try:
    logger, _ = configure_logging("dividend.log", "dividend")
    print(f"Log File Set! for dividend")
except:
    print(f"Logger Setting Error")



def dividend_data_to_database(final_data):
    logger.info("Connecting to the database...")
    
    connection = None
    cursor = None
    
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        logger.info("Database connection established.")
        
        cursor = connection.cursor(dictionary=True)
        current_datetime = datetime.now(pytz.timezone('Asia/Kathmandu')).strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('SELECT * FROM stock')
        stock_rows = cursor.fetchall()

        logger.info("Inserting/updating Dividend Data")
        if stock_rows:
            for row in final_data:
                # Debugging: Print the structure and type of `row`
                logger.debug(f"Processing row: {row}, Type of row: {type(row)}")
                
                # Debugging: Ensure row is a dictionary
                if not isinstance(row, dict):
                    logger.error(f"Row is not a dictionary: {row}")
                    continue

                stock = next((stock for stock in stock_rows if stock['symbol'] == row['symbol']), None)

                if stock:
                    try:
                        stock_id = stock['id']
                        fiscal_year = row['fiscal_year']  # Ensure fiscal_year is treated as a string

                        # Correctly parse and convert the cash_dividend and bonus_share fields
                        cash_dividend = row['cash_dividend'].replace('%', '').strip()
                        bonus_share = row['bonus_share'].replace('%', '').strip()

                        # Convert empty strings to None
                        cash_dividend = float(cash_dividend) if cash_dividend else 0.0
                        bonus_share = float(bonus_share) if bonus_share else 0.0

                        right_share = row['right_share']  # Ensure right_share is treated as a string

                        sql_dividend = """
                            INSERT INTO dividend (stock_id, fiscal_year, cash_dividend, bonus_share, right_share, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            fiscal_year = VALUES(fiscal_year),
                            cash_dividend = VALUES(cash_dividend),
                            bonus_share = VALUES(bonus_share),
                            right_share = VALUES(right_share),
                            updated_at = VALUES(updated_at);
                        """
                        cursor.execute(sql_dividend, (
                            stock_id,
                            fiscal_year,
                            cash_dividend,
                            bonus_share,
                            right_share,
                            current_datetime,
                            current_datetime
                        ))
                    except KeyError as e:
                        logger.error(f"Key error: {e} in row: {row}")
                    except Exception as error:
                        logger.error(f'Error during Dividend Data Insertion: {error}')
                else:
                    logger.warning(f"Skipping row due to missing stock: {row}")

            connection.commit()  # Ensure the transaction is committed
        else:
            logger.warning("No stock data found")

    except Exception as e:
        logger.error(f"Connection to database failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()
            logger.info("Database connection closed.")


def fetch_stock_symbols():
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM stock")
        stocks = cursor.fetchall()
        stock_symbols = [symbol for (symbol,) in stocks]
        cursor.close()
        conn.close()
        return stock_symbols
    except mysql.connector.Error as err:
        logger.error(f"Error fetching stock symbols: {err}")
        sys.exit(1)
 

async def scrape_dividend_data(symbol):
    logger.info(f"Starting scraping for symbol: {symbol}")
    
    # Set up browser options
    browser = await create_browser()
    page = await browser.newPage()

    try:
        # Open the website
        website = f'https://merolagani.com/CompanyDetail.aspx?symbol={symbol}'
        await page.goto(website)
        logger.info(f"Opened website: {website}")

        # Clicking Dividend Tab
        dividend_click = await page.waitForXPath('//*[@id="ctl00_ContentPlaceHolder1_CompanyDetail1_lnkDividendTab"]', timeout=60000)
        await page.evaluate('(element) => element.scrollIntoView()', dividend_click)
        await page.waitFor(2000)  # Wait for 2 seconds
        await dividend_click.click()
        logger.info("Clicked on the Dividend Tab")

        try:
            # Wait for the table to load and extract headers
            await page.waitForXPath('//*[@id="ctl00_ContentPlaceHolder1_CompanyDetail1_divDividendData"]/div[2]/table/tbody/tr[1]', timeout=60000)
            await page.waitFor(10000)  # Wait for 10 seconds
            headers = await page.xpath('//*[@id="ctl00_ContentPlaceHolder1_CompanyDetail1_divDividendData"]/div[2]/table/tbody/tr[1]')
            header = ["symbol"]  # Add symbol as the first header
            for row in headers:
                cells = await row.xpath('.//th[position()>1]')
                cell_data = [
                    await page.evaluate('(element) => element.textContent', cell)
                    for cell in cells
                ]
                cell_data = [
                    cell.strip().lower()
                    .replace('Fiscal Year', "fiscal_year")
                    .replace('Cash Dividend', 'cash_dividend')
                    .replace('Bonus Share', 'bonus_share')
                    .replace('Right Share', "right_share")
                    .replace(' ', '_')  # Replace spaces with underscores
                    for cell in cell_data
                ]
                header.extend(cell_data)
                logger.info(f"Extracted headers: {header}")
        except Exception as e:
            logger.error(f"Header Element Not Found Or Dividend Doesn't Exists for symbol {symbol}")
            logger.error(f"Header Table not present, Timeout Occurred for Finding Headers.")
            return

        all_data = []
        page_number = 1

        await page.waitFor(4000)  # Wait for 4 seconds
        
        while True:
            logger.info(f"Processing page {page_number} for Symbol/Company {symbol}")
            try:
                # Extract data from the current page
                rows = await page.xpath('//*[@id="ctl00_ContentPlaceHolder1_CompanyDetail1_divDividendData"]/div[2]/table/tbody/tr[position()>1]')
                for row in rows:
                    cells = await row.xpath('.//td[position()>1]')
                    cell_data = [
                        await page.evaluate('(element) => element.textContent', cell)
                        for cell in cells
                    ]
                    cell_data = [cell.strip() for cell in cell_data]
                    cell_data.insert(0, symbol)  # Insert the symbol at the beginning of the row
                    all_data.append(cell_data)
                logger.info(f"Extracted data: {all_data}")

                try:
                    logger.info("Checking For Next Button/More Data")
                    button = await page.waitForXPath(
                        '//*[@id="ctl00_ContentPlaceHolder1_CompanyDetail1_divDividendData"]/div[1]/div[2]/a[contains(@title, "Next Page")]',
                        {'visible': True, 'timeout': 60000}
                    )
                    logger.info("Next page button found")

                    # Scroll to the button
                    await page.evaluate('(element) => element.scrollIntoView({block: "center"})', button)
                    logger.info("Scrolled to the next page button")

                    # Wait for any animations
                    await page.waitFor(2000)

                    # Try to click the button
                    await button.click()
                    logger.info("Clicked the next page button")

                    page_number += 1
                    await page.waitFor(10000)  # Wait for 10 seconds after clicking
                except TimeoutError:
                    logger.error("No Next Button Found")
                    logger.error(f"No More Data for {symbol}")
                    break
                except Exception as e:
                    logger.error(f"Error navigating to the next page: {e}")
                    break
            except:
                logger.error("Error occurred while getting rows.")
                break
        
        # Convert list of lists to list of dictionaries
        if header and all_data:
            final_data = [dict(zip(header, row)) for row in all_data]
            logger.info(f"Final data converted to dictionaries")
            dividend_data_to_database(final_data) 
        else:
            return []
    except KeyboardInterrupt:
        logger.warning("Data extraction interrupted by user.")
        if browser:
            await close_browser(browser, page)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        if browser:
            await close_browser(browser, page)
    finally:
        await close_browser(browser, page)


def process_symbol(symbol):
    try:
        asyncio.run(scrape_dividend_data(symbol))
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

def pool_process():
    symbols = fetch_stock_symbols()

    with ProcessPoolExecutor(max_workers=6) as executor:
        executor.map(process_symbol, symbols)

def job():
    pool_process()


def dividend():
    while True:
        try:
            logger.info("Initializing schedule_jobs for Dividend...")
            # Schedule the job to run at 11:20 AM every day
            schedule.every().day.at("12:31").do(job)
            logger.info("Job scheduled successfully For Dividend.")

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
    dividend()
