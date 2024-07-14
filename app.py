import os
import sys
from flask import Flask, render_template, request
from concurrent.futures import ProcessPoolExecutor
from flask.logging import default_handler

# Append the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '', '..')))
from log import configure_logging

#Scripts Imports
from scripts.openClose import marketStatus
from scripts.live_nepse_data_scrapper import live_indexes
from scripts.epsPupeeter import eps
from scripts.funcDivident import dividend
from scripts.StockLive import LiveStockPrices
from scripts.nepseFloorsheet import dailyFloorsheet


# Ensure the logs directory exists
logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(logs_dir, exist_ok=True)

#Enabling Flask App
app = Flask(__name__)

# Remove default Flask logger
app.logger.removeHandler(default_handler)

# Configure custom logger for Flask app
flask_logger, _ = configure_logging("app.log", "flask_app")
app.logger = flask_logger


# <--- App Routes --->
@app.route('/')
def home():
    app.logger.info("Home page accessed.")
    return render_template('home.html')

# <--- Logs Routes --->
@app.route('/logs', methods=['POST'])
def show_logs():
    log_type = request.form.get('log_type')

    if log_type == 'marketOpenClose':
        log_content = get_market_status()
    elif log_type == 'live_indexes':
        log_content = live_indxes_status()
    elif log_type == 'eps':
        log_content = eps_status()
    elif log_type == 'dividend':
        log_content = dividend_status()
    elif log_type == 'live_stock':
        log_content = live_stock_status()
    elif log_type == 'floorsheet':
        log_content = floorsheet_status()
    else:
        log_content = "Select a log type above."
    return render_template('home.html', log_content=log_content)

# <--- Log Reading Functions --->
def get_market_status():
    return read_log_file('marketOpenClose.log')

def live_indxes_status():
    return read_log_file('nepseIndex.log')

def eps_status():
    return read_log_file('EPS.log')

def dividend_status():
    return read_log_file('dividend.log')

def live_stock_status():
    return read_log_file('stockLive.log')

def floorsheet_status():
    return read_log_file('floorsheet.log')


def read_log_file(file_name):
    file_path = os.path.join(logs_dir, file_name)
    try:
        with open(file_path, 'r') as file:
            log_content = file.read()
    except FileNotFoundError:
        app.logger.error(f"Log file '{file_path}' not found.")
        log_content = f"Log file '{file_path}' not found."
    return log_content


# <--- Running Scripts Function --->
def run_script(script):
    app.logger.info(f'Starting scripts {script.__name__}')
    script()


if __name__ == "__main__":

    # <--- Available Scripts  --->
    scripts = [marketStatus, live_indexes, eps, dividend, LiveStockPrices, dailyFloorsheet]


    # Create a ProcessPoolExecutor within the main block
    with ProcessPoolExecutor(max_workers=len(scripts)) as process_pool:
        # Submit each script to the process pool
        futures = [process_pool.submit(run_script, script) for script in scripts]

        try:
            app.run(debug=False, port=8000)
        finally:
            # Ensure the process pool is shut down properly
            process_pool.shutdown(wait=True)
