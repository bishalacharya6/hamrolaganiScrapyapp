import os
import json
from datetime import datetime, time as dt_time, timedelta
import asyncio
import sys
import psutil
import schedule
from pyppeteer import launch
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import time
import logging
from scripts.marketcheck import scrape_market_status
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from browser.broswerFunc import close_browser, create_browser


#Log File Set
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from log import configure_logging

try:
    logger, _ = configure_logging("nepseIndex.log", "live_Indexes")
    print(f"Index Log File Set!")
except:
    print(f"Logger Setting Error")

# Load environment variables
load_dotenv()

# URL of the site to scrape
url = 'https://www.sharesansar.com/live-trading'


browser_process_pids = []

async def scrape_website():
    browser = None 

    logger.info("Setting up the Pyppeteer Browser...")
    browser = await create_browser() 
    page = await browser.newPage()
    await page.goto(url)

    try:
        # Wait for the main container to ensure the page is loaded
        await page.waitForSelector('.bx-viewport', timeout=60000)

        # Define a list of all index names to search for
        index_names = [
            'NEPSE Index', 'Non Life Insurance', 'Others Index', 'Sensitive Float Inde.',
            'Sensitive Index', 'Trading Index', 'Banking SubIndex', 'Development Bank Ind.',
            'Finance Index', 'Float Index', 'Hotels And Tourism', 'HydroPower Index',
            'Investment', 'Life Insurance', 'Manufacturing And Pr.', 'Microfinance Index',
            'Mutual Fund'
        ]

        extracted_data = []

        for name in index_names:
            try:
                parent_container_xpath = '/html/body/div[2]/div/section[2]/div[3]/div/div/div/div/div[1]/div[3]/div[1]/div/div/div/div[1]/div'
                await page.waitForXPath(parent_container_xpath, timeout=60000)

                index_elements = await page.xpath(f"{parent_container_xpath}//div[h4/text()='{name}']")

                found_matching_element = False

                for index_element in index_elements:
                    found_matching_element = True

                    turnover_element = await index_element.querySelector('p.mu-price')
                    turnover = await page.evaluate('(element) => element.textContent', turnover_element)
                    turnover = turnover.strip()

                    index_value_element = await index_element.querySelector('p span.mu-value')
                    index_value = await page.evaluate('(element) => element.textContent', index_value_element)
                    index_value = index_value.strip()

                    percentage_change_element = await index_element.querySelector('p span.mu-percent')
                    percentage_change_text = await page.evaluate('(element) => element.textContent', percentage_change_element)
                    percentage_change_str = percentage_change_text.split('%')[0] + '%'
                    percentage_change = percentage_change_str.strip().replace('\n', '')

                    # Clean and convert data
                    turnover_cleaned = float(turnover.replace(',', '')) if turnover else 0.0
                    index_value_cleaned = float(index_value.replace(',', '')) if index_value else 0.0
                    percentage_change_cleaned = float(percentage_change.replace('%', '')) if percentage_change else 0.0

                    extracted_data.append({
                        'index_name': name,
                        'turnover': turnover_cleaned,
                        'last_trading_index': index_value_cleaned,
                        'percentage_change': percentage_change_cleaned
                    })

                    logger.debug(f"Index Name: {name}, Turnover: {turnover_cleaned}, Last Trading Index: {index_value_cleaned}, Percentage Change: {percentage_change_cleaned}")
                    break

                if not found_matching_element:
                    while True:
                        button = await page.waitForXPath('/html/body/div[2]/div/section[2]/div[3]/div/div/div/div/div[1]/div[3]/div[1]/div/div/div/div[2]/div/a[2]', timeout=10000)
                        await button.click()
                        await page.waitForTimeout(1000)  # Wait for the page to update

                        index_elements = await page.xpath(f"{parent_container_xpath}//div[h4/text()='{name}']")

                        for index_element in index_elements:
                            found_matching_element = True

                            turnover_element = await index_element.querySelector('p.mu-price')
                            turnover = await page.evaluate('(element) => element.textContent', turnover_element)
                            turnover = turnover.strip()

                            index_value_element = await index_element.querySelector('p span.mu-value')
                            index_value = await page.evaluate('(element) => element.textContent', index_value_element)
                            index_value = index_value.strip()

                            percentage_change_element = await index_element.querySelector('p span.mu-percent')
                            percentage_change_text = await page.evaluate('(element) => element.textContent', percentage_change_element)
                            percentage_change_str = percentage_change_text.split('%')[0] + '%'
                            percentage_change = percentage_change_str.strip().replace('\n', '')

                            # Clean and convert data
                            turnover_cleaned = float(turnover.replace(',', '')) if turnover else 0.0
                            index_value_cleaned = float(index_value.replace(',', '')) if index_value else 0.0
                            percentage_change_cleaned = float(percentage_change.replace('%', '')) if percentage_change else 0.0

                            extracted_data.append({
                                'index_name': name,
                                'turnover': turnover_cleaned,
                                'last_trading_index': index_value_cleaned,
                                'percentage_change': percentage_change_cleaned
                            })

                            logger.debug(f"Index Name: {name}, Turnover: {turnover_cleaned}, Last Trading Index: {index_value_cleaned}, Percentage Change: {percentage_change_cleaned}")
                            break

                        if found_matching_element:
                            break

            except Exception as e:
                logger.error(f"Error extracting data for '{name}': {e}")

    except Exception as e:
        logger.error(f"Error during web scraping: {e}")

    finally:
        # Close the browser
        await close_browser(browser, page)
        logger.info("Closed the browser.")
    
    logger.info("Data Extracted, Moving to Database")
    return extracted_data


def insert_data_into_database(final_data):
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        logger.info("Connection established.")
        cursor = connection.cursor()

        # Prepare the insert statement with placeholders
        sql = """
            INSERT INTO live_indices_price (index_id, last_trading_price, percentage_change, created_at, updated_at, turnover)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_trading_price = VALUES(last_trading_price),
                percentage_change = VALUES(percentage_change),
                turnover = VALUES(turnover),
                updated_at = VALUES(updated_at);
        """

        # Retrieve stock symbols and their IDs
        cursor.execute("SELECT id, index_display_name FROM sector")
        symbols = cursor.fetchall()

        index_dict = {index_name: index_id for index_id, index_name in symbols}

        # Insert data into the database
        for row in final_data:
            indexName = row['index_name']
            turnover = row['turnover']
            lastTradingPrice = row['last_trading_index']
            percentChange = row['percentage_change']
            
            index_id = index_dict.get(indexName)
            if index_id is not None:
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                values = (index_id, lastTradingPrice, percentChange, now, now, turnover)
                cursor.execute(sql, values)

        # Commit the transaction
        connection.commit()
        logger.info("Data committed to the database.")

    except mysql.connector.Error as err:
        logger.error(f"Error: {err}")
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.warning("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.warning("Database does not exist")
        else:
            logger.error(err)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("Closed the database connection.")


def job():
    current_time = datetime.now().time()
    market_close_time = dt_time(15, 5)  # 3:05 PM
    is_live = asyncio.run(scrape_market_status())
    
    # Log current market status and time
    logger.info(f"Market live status: {'True' if is_live else 'False'}, Current time: {current_time}\n")

    # Run the scraper if the market is live or if it's between 3:01 PM and 3:05 PM
    if is_live or (dt_time(15, 1) <= current_time < market_close_time):
        logger.info("Starting the scraping process...")
        try:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            extracted_data = loop.run_until_complete(scrape_website())
            
            logger.info("Scraping process completed.")
            if extracted_data:
                logger.info("Inserting data into the database...")
                insert_data_into_database(extracted_data)
                logger.info("Data insertion completed. \n\n")
            else:
                logger.info("No data extracted.")
        
        except Exception as e:
            logger.error(f"An error occurred during the scraping process in Job Function: {e}")


def live_indexes():
    while True:
        try:
            logger.info("Initializing schedule_jobs for Live Indexes...")
            start_time = dt_time(11, 00)
            end_time = dt_time(15, 5)

            schedule.every(0.5).minutes.do(job)
            logger.info("Job scheduled successfully.")

            while True:
                current_time = datetime.now().time()
                current_day = datetime.now().weekday()

                if current_day in [6, 0, 1, 2, 3] and start_time <= current_time <= end_time:
                    logger.info(f"Current time is {current_time}. Running scheduled jobs.")
                    schedule.run_pending()

                time.sleep(10)  # Check every 10 seconds

        except Exception as e:
            logger.error(f"An unexpected error occurred in schedule_jobs: {e}")
            logger.info("Attempting to restart schedule_jobs after a brief pause...")
            time.sleep(60) # Wait for 60 seconds before trying again
        except KeyboardInterrupt as k:
            logger.error(f"Keyboard Interrupted, Exiting the program.")
            break  


if __name__ == "__main__":
    live_indexes()