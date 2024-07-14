
import psutil
from pyppeteer import launch
import logging


# Setup logging
logger = logging.getLogger()


broswer_path = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
browser_process = None
browser_process_pids = []

async def create_browser():
    global browser_process_pids
    browser = await launch(
        headless=True,
        executablePath=broswer_path,
        args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-infobars',
              '--window-position=0,0', '--ignore-certificate-errors',
              '--ignore-certificate-errors-spki-list']
    )
    main_process = psutil.Process(browser.process.pid)
    # Add the main browser PID
    browser_process_pids.append(main_process.pid)
    # Add child process PIDs (tabs, etc.)
    for child in main_process.children(recursive=True):
        browser_process_pids.append(child.pid)
    logger.info(f"Browser launched with PIDs: {browser_process_pids}")
    return browser

async def close_browser(browser, page):
    global browser_process_pids

    closed = False

    if browser:
        try:
            # Close the page first
            try:
                await page.close()
                logger.info('Page closed')
            except Exception as e:
                logger.error(f"Error closing page: {e}")

            # Attempt to close the browser gracefully
            try:
                await browser.close()
                closed = True
                logger.info('Browser closed gracefully')
            except Exception as e:
                logger.error(f"Error closing browser gracefully: {e}")

            # Disconnect the browser
            try:
                await browser.disconnect()
                logger.info('Browser disconnected')
            except Exception as e:
                logger.error(f"Error disconnecting browser: {e}")

        except Exception as e:
            logger.error(f"Error handling browser: {e}")

    # If the browser didn't close gracefully, use forceful measures
    if not closed:
        logger.info('Attempting to close browser forcefully')

        for pid in browser_process_pids:
            try:
                browser_process = psutil.Process(pid)
                if browser_process.is_running():
                    try:
                        browser_process.terminate()
                        logger.info(f'Browser process {pid} terminated')
                        try:
                            browser_process.wait(timeout=5)
                        except psutil.TimeoutExpired:
                            logger.warning(f"Timeout expired while waiting for browser process {pid} to terminate. Killing process.")
                            browser_process.kill()
                            browser_process.wait(timeout=5)
                            logger.info(f'Browser process {pid} killed')
                    except Exception as e:
                        logger.error(f"Error terminating browser process {pid}: {e}")
                else:
                    logger.info(f'Browser process {pid} already terminated')
            except psutil.NoSuchProcess:
                logger.info(f'Browser process {pid} does not exist')
            except Exception as e:
                logger.error(f"Error handling browser process {pid}: {e}")

    # Final check to ensure closure
    for pid in browser_process_pids:
        try:
            browser_process = psutil.Process(pid)
            if browser_process.is_running():
                logger.error(f"Final Check - Failed to close browser process {pid} by all means")
            else:
                logger.info(f"Final Check - Browser process {pid} is closed")
        except psutil.NoSuchProcess:
            logger.info(f"Final Check - Browser process {pid} does not exist")

    # Clear the PID list
    browser_process_pids = []

