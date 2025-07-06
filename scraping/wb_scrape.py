import tempfile, uuid, shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
import os
import requests
from time import sleep
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import csv

# Specify the output CSV file
output_file = "player_data.csv"

def new_driver() -> webdriver.Chrome:
    """Return a fresh, headless Chrome with its own throw‑away profile."""
    opts = Options()
    opts.add_argument("--headless=new")          # headless Chrome ≥115
    opts.add_argument("--no-sandbox")            # good practice on EC2
    opts.add_argument("--disable-dev-shm-usage") # avoid /dev/shm issues

    # each run gets a unique profile so the 'user‑data‑dir already in use'
    # lock files can never collide
    profile_dir = tempfile.mkdtemp(prefix="chrome-")
    opts.add_argument(f"--user-data-dir={profile_dir}")

    driver = webdriver.Chrome(options=opts)
    driver._profile_dir = profile_dir           # stash so we can delete it
    return driver


def close_driver(driver: webdriver.Chrome):
    """Quit Chrome and clean up the temporary profile directory."""
    try:
        driver.quit()
    finally:
        shutil.rmtree(getattr(driver, "_profile_dir", ""), ignore_errors=True)

# Write data to the CSV
def save_data(timestamp, player_data, dir):

  os.makedirs(dir, exist_ok = True)
  output_file = timestamp + ".csv"
  output_path = os.path.join(dir,output_file)
  if os.path.exists(output_path):
    print("File exists!")
    return

  with open(output_path, mode='w', newline='', encoding='utf-8') as file:
      writer = csv.writer(file)

      # Write the header (keys from the first dictionary in player_data)
      if player_data:  # Check if the list is not empty
          header = player_data[0].keys()
          writer.writerow(header)

          # Write each player's data (values)
          for player in player_data:
              writer.writerow(player.values())
  print(timestamp, " saved!")
  return


def scrape(driver, url, timestamp, season):
  print(f"url: {url}")
  driver.get(url)
  slider = driver.find_element(By.ID, 'filter-slider')
  #slider = WebDriverWait(driver, 15).until(
  #      EC.visibility_of_all_elements_located((By.ID, 'filter-slider'))
  #  )

  # Use JavaScript to set the slider value and dispatch input and change events
  desired_value = 1
  driver.execute_script("""
      var slider = arguments[0];
      slider.value = arguments[1];
      slider.dispatchEvent(new Event('input'));
      slider.dispatchEvent(new Event('change'));
  """, slider, desired_value)


  # checkboxes = driver.find_elements(By.CLASS_NAME, "year-checkbox")
  # for checkbox in checkboxes:
  #   if not checkbox.is_selected():
  #       checkbox.click()
  # for checkbox in checkboxes:
  #   print(f"Checkbox for {checkbox.get_attribute('year')} is {'checked' if checkbox.is_selected() else 'unchecked'}.")

  # change the dropdown to Regular season
  wait = WebDriverWait(driver, 10)
  dropdown_element = wait.until(
      EC.presence_of_element_located((By.ID, "filter-season-type"))
  )

  # Create a Selenium Select object based on the dropdown
  select_dropdown = Select(dropdown_element)

  # Select by visible text
  select_dropdown.select_by_visible_text(season)

  # (Optional) Print out the newly selected option
  selected_option = select_dropdown.first_selected_option.text
  print("Selected option:", selected_option)



  # Wait for the table to update (use WebDriverWait if necessary for AJAX loads)
  #WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.XPATH, "//tr[@data-key]")))

  # Locate all rows in the table
  rows = driver.find_elements(By.XPATH, "//tr[@data-key]")

  upper = ['row_num', 'name', 'team', 'pos', 'mp','rap_box_o', 'rap_box_d', 'rap_box', 'rap_onoff_o', 'rap_onoff_d', 'rap_onoff', 'rap_o', 'rap_d', 'rap', 'war']
  # Initialize an empty list to store player data
  player_data = []

  ids = []

  # Loop through each row and gather all data
  for row in rows:
      player = {}
      # Extract key attributes
      player['data_key'] = row.get_attribute('data-key')
      player['id'] = row.get_attribute('id')

      # Extract all columns, even if empty
      columns_td = row.find_elements(By.TAG_NAME, 'td')
      for idx, col in enumerate(columns_td):
          text = col.text.strip()  # Extract text content
          if not text:  # Handle empty cells
              text = col.get_attribute('data-val') or ''  # Try to fetch 'data-val' if available
          player[upper[idx]] = text

      # Add the player data to the list
      player_data.append(player)

  dir = season
  save_data(timestamp, player_data, season)

def fetch_wayback_snapshots(url):
    """
    Fetch a list of snapshots for the given URL using
    the Wayback Machine CDX server API. Returns a list
    of dictionaries with timestamp and the complete
    archived URL.
    """
    # CDX API endpoint with JSON output
    missing_timestamps = ['20221027170035', '20220611185233', '20250306125347', '20230516130653', '20250102180841',
                          '20230427031018', '20211220130617', '20210712174807', '20220622193644', '20220527213923',
                          '20220328193951', '20230427032049', '20211216233434', '20231031172208', '20250306030046',
                          '20230427031254', '20231203213544', '20220723033222', '20220321165347', '20220405032451',
                          '20220104004934', '20210930085104', '20230427030700', '20230427030245', '20230427030630',
                          '20230427032439', '20230331204701', '20221227232407', '20240119013336', '20230427030111',
                          '20230125235613', '20230427030648', '20211228215345', '20220922202824', '20220218020704',
                          '20230110171212', '20230130181128', '20220413162204', '20210613164620', '20210713150229',
                          '20220313045617', '20230427025720', '20230427032414', '20220219042241', '20231223025040',
                          '20230427030030', '20220107000332', '20211217073728', '20230427031418', '20221219133756',
                          '20211227193849', '20230521135514', '20230427030821', '20230111181221', '20220429155256',
                          '20230427031103', '20220211160504', '20230427031905', '20220202150808', '20230427033450',
                          '20230427031749', '20230427032056', '20230427030202', '20230427030946', '20230521135346',
                          '20230427030358', '20230315050931', '20230427031153', '20230427031405', '20230427030857',
                          '20240716020028', '20230726112335', '20230109161003', '20230427032259', '20230427030454',
                          '20230427031535', '20220228231924', '20230110170543', '20220414132338', '20220313034447',
                          '20220310180700', '20221216153824', '20210716133708', '20211218150806', '20230427032607',
                          '20221225025305', '20210613132344', '20230427033611', '20230427030131', '20230427030331',
                          '20220423094827', '20230516130201', '20221129223819', '20211223202134', '20230427032634',
                          '20230427033656', '20250306013802', '20211229052155', '20230413201712', '20230427032358',
                          '20220108110854', '20230427033532', '20230427031504', '20240111083822', '20230621145310',
                          '20230121202458', '20230427031531', '20230427030510', '20220310130132', '20230427033559',
                          '20230427033636', '20220511223358', '20230726161017', '20230427031242', '20221110201351',
                          '20230102040645', '20221205183628', '20211220222259', '20230705162722', '20250129230503',
                          '20230427030308', '20230427045833', '20230427031521', '20220518220811', '20220218030842',
                          '20230104165156', '20210706140744', '20220311180739', '20230516130208', '20230705172423',
                          '20230427031621', '20220215210507', '20221110125421', '20230427033717', '20230314232259',
                          '20230427031644', '20230427031036', '20230130180424', '20221202005041', '20211225192748',
                          '20230427030800', '20230427032456', '20230427031352', '20221105202519', '20221110094247',
                          '20230705202955', '20241002175457', '20221025125529', '20230427031130', '20230103103710',
                          '20230104165651', '20230705053912', '20220406185135', '20230427032736', '20241008080203',
                          '20230419112111', '20230705025214', '20220411135336', '20221219222129', '20221213020222',
                          '20230427043426', '20221130175609', '20221110090001', '20221216211010', '20220218194837',
                          '20230117203943', '20220509142356', '20230427033745', '20230217095605', '20220211161059',
                          '20221112202317', '20220617010746', '20230427032711', '20250304201139', '20230210144050',
                          '20230427032156', '20230427030557', '20230625193104', '20230120195118', '20230427030920',
                          '20221127174056', '20230427031327', '20230427032550', '20230328185315', '20230427031442',
                          '20230109160432', '20230427032647', '20250305140835', '20220617032052', '20230327160805',
                          '20230427032323', '20230113014403', '20230120044359', '20210916100351', '20220705205723',
                          '20220111131517', '20220112144241', '20230427031438', '20230427030404', '20230308040913',
                          '20240123040332', '20221208172539', '20230328235512', '20230427030740', '20220423135506',
                          '20230705052904', '20220412183957', '20230427030541', '20240519162623', '20221126212301',
                          '20220609034716', '20230330021102', '20250306182230', '20230427031601', '20230427030433',
                          '20230428084720', '20220217182257', '20230427033503', '20221120140346', '20230427032752',
                          '20230113025136', '20230125235051', '20250118082338', '20220120135952', '20220403123134',
                          '20231001010444', '20220324192516', '20221219230252', '20221222190945', '20220416005214',
                          '20230427025949', '20211227184248', '20220414120855', '20230508032223', '20220313224831',
                          '20240408182502', '20211227204848', '20220313063115', '20230427031721', '20230427030052',
                          '20220430055036', '20220215211049', '20230427032809', '20230306010123', '20220408230001']

    cdx_url = (
        "https://web.archive.org/cdx/search/cdx"
        "?url={url}&output=json"
        "&fl=timestamp,original,statuscode"
        "&filter=statuscode:200"
    ).format(url=url)

    try:
        response = requests.get(cdx_url)
        response.raise_for_status()

        data = response.json()

        # The first item in `data` is a list of headers (e.g., ["timestamp", "original", "statuscode"])
        if len(data) < 2:
            print("No snapshots found or no data returned.")
            return []

        headers = data[0]       # e.g. ["timestamp", "original", "statuscode"]
        snapshot_rows = data[1:]

        snapshots_list = []

        print(snapshot_rows)

        for row in snapshot_rows:
            snapshot_info = dict(zip(headers, row))
            timestamp = snapshot_info["timestamp"]
            if timestamp not in missing_timestamps:
                continue
            original_url = snapshot_info["original"]

            # Construct the Wayback Machine archived URL
            archived_url = f"https://web.archive.org/web/{timestamp}/{original_url}"

            # Add to our list
            snapshots_list.append({
                "timestamp": timestamp,
                "archived_url": archived_url
            })

        return snapshots_list

    except requests.exceptions.RequestException as e:
        print(f"Error fetching snapshots: {e}")
        return []

def main():
    target_url = "https://projects.fivethirtyeight.com/nba-player-ratings/"
    snapshots   = fetch_wayback_snapshots(target_url)
    failed      = []

    for snap in snapshots:
        for season in ["Full season", "Regular season", "Playoffs"]:
            outfile = f"{snap['timestamp']}.csv"
            if os.path.exists(os.path.join(season, outfile)):
                continue

            driver = new_driver()                       # <-- create
            try:
                scrape(driver, snap['archived_url'],
                       snap['timestamp'], season)
            except Exception as e:
                print(f"[{snap['timestamp']}] failed: {e}")
                failed.append(snap['timestamp'])
            finally:
                close_driver(driver)                    # <-- destroy

    print("Failed timestamps:", failed)


if __name__ == "__main__":
    main()
