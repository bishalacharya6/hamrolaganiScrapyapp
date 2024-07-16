import os
import sys
import psutil
import schedule
import time
import logging
import platform

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.', '..')))
from log import configure_logging

# Configure logging
try:
    logger, _ = configure_logging("chromeKiller.log", "chromeKiller")
    logger.info("Log File Set!")
except Exception as e:
    print(f"Logger Setting Error: {e}")
    sys.exit(1)

def kill():
    """
    Kills all Chrome processes running on the machine.
    """
    try:
        chrome_processes_killed = 0
        for proc in psutil.process_iter(['pid', 'name']):
            # Check if the process name contains 'chrome'
            if 'chrome' in proc.info['name'].lower():
                try:
                    process = psutil.Process(proc.info['pid'])
                    process.terminate()
                    chrome_processes_killed += 1
                    logger.info(f"Terminated Chrome process with PID: {proc.info['pid']}")
                except psutil.NoSuchProcess:
                    logger.warning(f"Process {proc.info['pid']} no longer exists.")
                except psutil.AccessDenied:
                    logger.error(f"Access denied to terminate process {proc.info['pid']}.")
                except Exception as e:
                    logger.error(f"Failed to terminate Chrome process with PID: {proc.info['pid']}. Error: {e}")

        if chrome_processes_killed == 0:
            logger.info("No Chrome processes found to terminate.")
        else:
            logger.info(f"Total Chrome processes terminated: {chrome_processes_killed}")

    except Exception as e:
        logger.error(f"An error occurred while terminating Chrome processes: {e}")

def chromeKiller():
    """
    Schedules the chromeKiller function to run every day at 12:58.
    """
    while True:
        try:
            schedule.every().day.at("13:02").do(kill)
            logger.info("Scheduled chromeKiller to run every day at 12:58.")

            while True:
                logger.info("Scheduler Running.")
                schedule.run_pending()
                time.sleep(30)  # Sleep for 30 seconds
        except Exception as e:
            logger.error(f"An error occurred in the scheduling loop: {e}")
            time.sleep(60)  # Wait for 1 minute before retrying
        except KeyboardInterrupt:
            logger.info("Chrome Killer script interrupted by user. Exiting.")
            break

if __name__ == "__main__":
    chromeKiller()