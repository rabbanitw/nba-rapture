import tempfile, uuid, shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
import os
import requests
from time import sleep
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    StaleElementReferenceException,
    WebDriverException,
    TimeoutException,
    NoSuchElementException
)
import csv
from pathlib import Path
from selenium.webdriver.common.action_chains import ActionChains
import time
import random
import logging
from functools import wraps

from scraping.data_saver import timestamp_already_processed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Specify the output CSV file
output_file = "player_data.csv"

# Retry configuration
MAX_RETRIES = 5
BASE_DELAY = 2  # seconds
MAX_DELAY = 60  # seconds


def exponential_backoff_retry(max_retries=MAX_RETRIES, base_delay=BASE_DELAY, max_delay=MAX_DELAY,
                              exceptions=(Exception,)):
    """Decorator for exponential backoff retry logic"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Final attempt failed for {func.__name__}: {e}")
                        raise

                    # Calculate delay with exponential backoff + jitter
                    delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}")
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
            return None

        return wrapper

    return decorator


def new_driver() -> webdriver.Chrome:
    """Return a fresh, headless Chrome with its own throw‑away profile."""
    opts = Options()
    opts.add_argument("--headless=new")  # headless Chrome ≥115
    opts.add_argument("--no-sandbox")  # good practice on EC2
    opts.add_argument("--disable-dev-shm-usage")  # avoid /dev/shm issues
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-images")  # Skip loading images for faster performance

    # each run gets a unique profile so the 'user‑data‑dir already in use'
    # lock files can never collide
    profile_dir = tempfile.mkdtemp(prefix="chrome-")
    opts.add_argument(f"--user-data-dir={profile_dir}")

    driver = webdriver.Chrome(options=opts)
    driver._profile_dir = profile_dir  # stash so we can delete it

    # Set timeouts
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)

    return driver


def close_driver(driver: webdriver.Chrome):
    """Quit Chrome and clean up the temporary profile directory."""
    try:
        if driver:
            driver.quit()
    except Exception as e:
        logger.warning(f"Error closing driver: {e}")
    finally:
        try:
            profile_dir = getattr(driver, "_profile_dir", "")
            if profile_dir and os.path.exists(profile_dir):
                shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception as e:
            logger.warning(f"Error cleaning up profile directory: {e}")


def check_wayback_url_accessible(url, timeout=30):
    """Check if Wayback Machine URL is accessible via HTTP request"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        return response.status_code == 200
    except Exception as e:
        logger.warning(f"URL accessibility check failed for {url}: {e}")
        return False


@exponential_backoff_retry(
    max_retries=3,
    exceptions=(requests.exceptions.RequestException,)
)
def fetch_wayback_snapshots(url):
    """
    Fetch a list of snapshots for the given URL using
    the Wayback Machine CDX server API. Returns a list
    of dictionaries with timestamp and the complete
    archived URL.
    """
    cdx_url = (
        "https://web.archive.org/cdx/search/cdx"
        "?url={url}&output=json"
        "&fl=timestamp,original,statuscode"
        "&filter=statuscode:200"
    ).format(url=url)

    response = requests.get(cdx_url, timeout=30)
    response.raise_for_status()

    data = response.json()

    # The first item in `data` is a list of headers (e.g., ["timestamp", "original", "statuscode"])
    if len(data) < 2:
        logger.info("No snapshots found or no data returned.")
        return []

    headers = data[0]  # e.g. ["timestamp", "original", "statuscode"]
    snapshot_rows = data[1:]

    snapshots_list = []

    for row in snapshot_rows:
        snapshot_info = dict(zip(headers, row))
        timestamp = snapshot_info["timestamp"]

        if timestamp_already_processed(timestamp, "538"):
            logger.info(f"Skipping already processed timestamp: {timestamp}")
            continue
        else:
            logger.info(f"Found timestamp: {timestamp}")

        original_url = snapshot_info["original"]

        # Construct the Wayback Machine archived URL
        archived_url = f"https://web.archive.org/web/{timestamp}/{original_url}"

        # Add to our list
        snapshots_list.append({
            "timestamp": timestamp,
            "archived_url": archived_url
        })

    return snapshots_list


# Write data to the CSV
def save_data(timestamp, player_data, season, checkbox_id):
    try:
        base_dir = Path(__file__).resolve().parent
        dir_538 = base_dir / "538"
        new_dir = dir_538 / season / checkbox_id
        new_dir.mkdir(parents=True, exist_ok=True)
        new_path = os.path.join(new_dir, f"{timestamp}.csv")

        if os.path.exists(new_path):
            logger.info(f"File {new_path} already exists!")
            return

        with open(new_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # Write the header (keys from the first dictionary in player_data)
            if player_data:  # Check if the list is not empty
                header = player_data[0].keys()
                writer.writerow(header)

                # Write each player's data (values)
                for player in player_data:
                    writer.writerow(player.values())
        logger.info(f"Timestamp {timestamp} and season {season} saved successfully!")

    except Exception as e:
        logger.error(f"Error saving data for timestamp {timestamp}: {e}")
        raise


@exponential_backoff_retry(
    max_retries=3,
    exceptions=(WebDriverException, TimeoutException, NoSuchElementException)
)
def safe_navigate_to_url(driver, url):
    """Safely navigate to URL with retry logic"""
    logger.info(f"Navigating to: {url}")
    driver.get(url)

    # Wait for page to load and check if it's the error page
    time.sleep(3)

    # Check for Wayback Machine error indicators
    page_source = driver.page_source.lower()
    error_indicators = [
        "wayback machine doesn't have that page archived",
        "page cannot be displayed",
        "error 404",
        "not found"
    ]

    for indicator in error_indicators:
        if indicator in page_source:
            raise WebDriverException(f"Page not properly archived: {indicator}")

    logger.info("Successfully navigated to URL")


@exponential_backoff_retry(
    max_retries=3,
    exceptions=(WebDriverException, TimeoutException, NoSuchElementException)
)
def safe_find_element(driver, by, value, timeout=10):
    """Safely find element with retry logic"""
    wait = WebDriverWait(driver, timeout)
    return wait.until(EC.presence_of_element_located((by, value)))


@exponential_backoff_retry(
    max_retries=3,
    exceptions=(WebDriverException, TimeoutException)
)
def unselect_all_checkboxes(driver):
    js_unselect_all = """
    var checkboxes = document.querySelectorAll('.year-checkbox');
    checkboxes.forEach(function(checkbox) {
        if (checkbox.checked) {
            checkbox.checked = false;
            checkbox.dispatchEvent(new Event('change', { bubbles: true }));
            checkbox.dispatchEvent(new Event('click', { bubbles: true }));
        }
    });
    """
    driver.execute_script(js_unselect_all)
    logger.info("Unselected all checkboxes")


@exponential_backoff_retry(
    max_retries=3,
    exceptions=(WebDriverException, TimeoutException)
)
def select_checkbox_by_id(driver, checkbox_id):
    js_select = f"""
    var checkbox = document.getElementById('{checkbox_id}');
    if (checkbox && !checkbox.checked) {{
        checkbox.checked = true;
        checkbox.dispatchEvent(new Event('change', {{ bubbles: true }}));
        checkbox.dispatchEvent(new Event('click', {{ bubbles: true }}));
        return true;
    }}
    return false;
    """
    result = driver.execute_script(js_select)
    if result:
        logger.info(f"Selected checkbox: {checkbox_id}")
    else:
        raise WebDriverException(f"Failed to select checkbox: {checkbox_id}")


def fetch_and_save_data(driver):
    """Extract player data from the current page"""
    try:
        player_data = []

        # Wait for data to load
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.XPATH, "//tr[@data-key]")))

        rows = driver.find_elements(By.XPATH, "//tr[@data-key]")

        if not rows:
            raise NoSuchElementException("No player data rows found")

        upper = ['row_num', 'name', 'team', 'pos', 'mp', 'rap_box_o', 'rap_box_d', 'rap_box', 'rap_onoff_o',
                 'rap_onoff_d',
                 'rap_onoff', 'rap_o', 'rap_d', 'rap', 'war']

        # Loop through each row and gather all data
        for row in rows:
            try:
                player = {}
                # Extract key attributes
                player['data_key'] = row.get_attribute('data-key')
                player['id'] = row.get_attribute('id')

                # Extract all columns, even if empty
                columns_td = row.find_elements(By.TAG_NAME, 'td')
                for idx, col in enumerate(columns_td):
                    if idx < len(upper):  # Prevent index errors
                        text = col.text.strip()  # Extract text content
                        if not text:  # Handle empty cells
                            text = col.get_attribute('data-val') or ''  # Try to fetch 'data-val' if available
                        player[upper[idx]] = text

                # Add the player data to the list
                player_data.append(player)

            except Exception as e:
                logger.warning(f"Error processing row: {e}")
                continue

        logger.info(f"Successfully extracted {len(player_data)} player records")
        return player_data

    except Exception as e:
        logger.error(f"Error fetching player data: {e}")
        raise


def scrape(driver, url, timestamp, season, checkbox_id):
    """Main scraping function with robust error handling"""
    logger.info(f"Starting scrape for timestamp {timestamp}, season {season}, checkbox {checkbox_id}")

    # Navigate to URL with retry logic
    safe_navigate_to_url(driver, url)

    # Set slider value
    slider = safe_find_element(driver, By.ID, 'filter-slider')
    desired_value = 1
    driver.execute_script("""
      var slider = arguments[0];
      slider.value = arguments[1];
      slider.dispatchEvent(new Event('input'));
      slider.dispatchEvent(new Event('change'));
    """, slider, desired_value)

    # Change the dropdown to selected season
    dropdown_element = safe_find_element(driver, By.ID, "filter-season-type")
    select_dropdown = Select(dropdown_element)
    select_dropdown.select_by_visible_text(season)

    selected_option = select_dropdown.first_selected_option.text
    logger.info(f"Selected season option: {selected_option}")

    # Wait for checkboxes to be present
    safe_find_element(driver, By.CLASS_NAME, "year-checkbox")

    logger.info(f"Processing checkbox {checkbox_id}")

    # Unselect all checkboxes
    unselect_all_checkboxes(driver)
    time.sleep(1)

    # Select the target checkbox
    select_checkbox_by_id(driver, checkbox_id)
    time.sleep(4)  # Allow page to update

    # Extract and save data
    player_data = fetch_and_save_data(driver)
    save_data(timestamp, player_data, season, checkbox_id)

    logger.info(f"Successfully completed scrape for {timestamp}")


def main():
    logger.info("Starting scraper application")

    target_url = "https://projects.fivethirtyeight.com/nba-player-ratings/"

    try:
        snapshots = fetch_wayback_snapshots(target_url)
        logger.info(f"Found {len(snapshots)} snapshots")

        # Remove duplicates
        seen_timestamps = set()
        unique_snapshots = []

        for snapshot in snapshots:
            if snapshot["timestamp"] not in seen_timestamps:
                seen_timestamps.add(snapshot["timestamp"])
                unique_snapshots.append(snapshot)

        snapshots = unique_snapshots
        logger.info(f"After deduplication: {len(snapshots)} unique snapshots")

        failed = []
        successful = []

        base_dir = Path(__file__).resolve().parent
        dir_538 = base_dir / "538"

        total_tasks = len(snapshots) * 3 * 2  # snapshots * seasons * checkbox_ids
        current_task = 0

        for snap in snapshots:
            # Pre-check URL accessibility
            if not check_wayback_url_accessible(snap['archived_url']):
                logger.warning(f"Skipping inaccessible URL: {snap['archived_url']}")
                failed.append(f"{snap['timestamp']}_url_inaccessible")
                continue

            for season in ["Full season", "Regular season", "Playoffs"]:
                checkbox_ids = [
                    'filter-2021',
                    'filter-2022'
                ]

                for checkbox_id in checkbox_ids:
                    current_task += 1
                    logger.info(f"Progress: {current_task}/{total_tasks}")

                    # Check if already processed
                    new_dir = dir_538 / season / checkbox_id
                    new_dir.mkdir(parents=True, exist_ok=True)

                    timestamp = snap['timestamp']
                    outfile = f"{timestamp}.csv"
                    file_path = os.path.join(new_dir, outfile)

                    if os.path.exists(file_path):
                        logger.info(f"Skipping already processed: {file_path}")
                        successful.append(f"{timestamp}_{season}_{checkbox_id}")
                        continue

                    driver = None
                    try:
                        driver = new_driver()
                        scrape(driver, snap['archived_url'], timestamp, season, checkbox_id)
                        successful.append(f"{timestamp}_{season}_{checkbox_id}")

                    except Exception as e:
                        error_msg = f"[{timestamp}_{season}_{checkbox_id}] failed: {e}"
                        logger.error(error_msg)
                        failed.append(f"{timestamp}_{season}_{checkbox_id}")

                    finally:
                        if driver:
                            close_driver(driver)

                        # Add delay between requests to be respectful
                        time.sleep(random.uniform(2, 5))

        # Final report
        logger.info(f"Scraping completed!")
        logger.info(f"Successful: {len(successful)}")
        logger.info(f"Failed: {len(failed)}")

        if failed:
            logger.warning("Failed items:")
            for item in failed:  # Show first 10 failures
                logger.warning(f"  - {item}")

    except Exception as e:
        logger.error(f"Critical error in main(): {e}")
        raise


if __name__ == "__main__":
    main()