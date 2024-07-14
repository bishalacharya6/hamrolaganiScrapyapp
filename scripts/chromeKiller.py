import os
import signal
import sys
import psutil
import schedule
import time
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from log import configure_logging

# Configure logging
try:
    logger, _ = configure_logging("chromeKiller.log", "chromeKiller")
    logger.info(f"Log File Set!")
except:
    logger.error(f"Logger Setting Error")

def chromeKiller():
    """
    Kills all Chrome processes running on the machine.
    """
    try:
        chrome_processes_killed = 0
        for proc in psutil.process_iter(['pid', 'name']):
            # Check if the process name contains 'chrome'
            if 'chrome' in proc.info['name'].lower():
                try:
                    os.kill(proc.info['pid'], signal.SIGKILL)
                    chrome_processes_killed += 1
                    logging.info(f"Killed Chrome process with PID: {proc.info['pid']}")
                except Exception as e:
                    logging.error(f"Failed to kill Chrome process with PID: {proc.info['pid']}. Error: {e}")

        if chrome_processes_killed == 0:
            logging.info("No Chrome processes found to kill.")
        else:
            logging.info(f"Total Chrome processes killed: {chrome_processes_killed}")

    except Exception as e:
        logging.error(f"An error occurred while killing Chrome processes: {e}")

def schedule_chromeKiller():
    """
    Schedules the chromeKiller function to run every day at 10 PM.
    """
    while True:
        try:
            schedule.every().day.at("22:00").do(chromeKiller)
            logging.info("Scheduled chromeKiller to run every day at 10 PM.")

            while True:
                try:
                    schedule.run_pending()
                    time.sleep(60)  # Sleep for 1 min
                except Exception as e:
                    logger.info(f"Unexpected Error Occured {e}")
                    break
        except Exception as e:
            logging.error(f"An error occurred in the scheduling loop: {e}")
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    try:
        logging.info("Starting chrome_killer script.")
        schedule_chromeKiller()
    except Exception as e:
        logging.error(f"An error occurred in the main script: {e}")
