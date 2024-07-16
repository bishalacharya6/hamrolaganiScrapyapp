import asyncio
import os
import logging
from datetime import datetime, time 
import sys
from dotenv import load_dotenv
import psutil
from pyppeteer import launch
import mysql.connector
from mysql.connector import Error
import schedule
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from browser.broswerFunc import close_browser, create_browser
from log import configure_logging


#Loggin Setting
try:
    logger, _ = configure_logging("marketOpenClose.log", "marketStatus")
    print(f"Market Status Log File Set!")
except:
    print(f"Logger Setting Error")


# Load environment variables
load_dotenv()


# Update market status in the database
def update_market_status(is_live):
    cursor = 0
    connection = 0
    try:
        # Create database connection
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )
        if connection.is_connected():
            logger.info("Database connection established successfully")
        
        cursor = connection.cursor()
        
        # Check if the record exists
        cursor.execute("SELECT * FROM application_config WHERE `key` = 'market_status'")
        result = cursor.fetchone()
        
        current_time = datetime.now()
        
        if result:
            # Update existing record
            update_query = """
            UPDATE application_config 
            SET `value` = %s, updated_at = %s 
            WHERE `key` = 'market_status'
            """
            cursor.execute(update_query, (is_live, current_time))
        else:
            # Insert new record
            insert_query = """
            INSERT INTO application_config (`key`, `value`, created_at, updated_at) 
            VALUES ('market_status', %s, %s, %s)
            """
            cursor.execute(insert_query, (is_live, current_time, current_time))
        
        connection.commit()
        logger.info(f"Market status updated: {'Live' if is_live else 'Not Live'} \n\n")
    except Error as e:
        logger.error(f"Error while updating database: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# Main scraping function
async def scrape_market_status():
    browser = None
    
    try:
        browser = await create_browser()
        page = await browser.newPage()
        
        # Set user agent
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        website = 'https://www.nepalstock.com.np/'
        
        try:
            await page.goto(website, {'waitUntil': 'networkidle0'})
            logger.info("Page loaded successfully")

            # Wait for the element to be visible
            await page.waitForXPath('/html/body/app-root/div/main/div/app-dashboard/div[1]/div[1]/div/div[1]/div[1]/div[2]/span[2]', {'visible': True, 'timeout': 60000})
            
            # Get the text from the element
            element = await page.xpath('/html/body/app-root/div/main/div/app-dashboard/div[1]/div[1]/div/div[1]/div[1]/div[2]/span[2]')
            market_status_text = await page.evaluate('(element) => element.textContent', element[0])
            
            logger.info(f"Retrieved market status text: {market_status_text}")

            # Check if the market is live
            is_live = 1 if "Live Market" in market_status_text else 0

            # Update the database
            update_market_status(is_live)

        except Exception as e:
            logger.error(f"An error occurred during scraping: {e}")

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Exiting...")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if browser:
            await close_browser(browser, page)

# Schedule the scraping function to run every day at 11:20 AM
def job():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scrape_market_status())
    loop.close()

def marketStatus():
    while True:
        try:
            logger.info("Initializing schedule_jobs for Market Status...")
            # Schedule the job to run at 11:20 AM every day
            schedule.every().day.at("11:00").do(job)
            schedule.every().day.at("11:01").do(job)
            schedule.every().day.at("11:02").do(job)
            schedule.every().day.at("11:03").do(job)
            schedule.every().day.at("15:00").do(job)
            schedule.every().day.at("15:01").do(job)
            schedule.every().day.at("15:02").do(job)
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
    marketStatus()
