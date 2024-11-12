from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
import shutil
import os
import time
import logging

# import logging

LONG_PAGE_DELAY = 2
SHORT_PAGE_DELAY = 1
BASE_DIR = os.curdir
MAX_ATTEMPTS = 5  # Maximum number of retry attempts
RETRY_DELAY = 2  # Time to wait before retrying after being blocked
YEAR_LIMITER = 1


URL = 'https://wyszukiwarka-krs.ms.gov.pl/'
test_krs = '0000573610'

test_krs_list = ['0000398281', '0000573610', '0000735160']


# # Set up logging configuration
# logging.basicConfig(
#     filename='web_driver_log.txt',
#     level=logging.DEBUG,  # Change to DEBUG for more detailed logging
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

def time_it(func):
    """Decorator to time the execution of a function."""

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"⏱️ Time taken for {func.__name__}: {end_time - start_time:.2f} seconds")
        return result

    return wrapper


def initialize_driver():

    port = 4445
    # Set directory to store the driver
    driver_path = "/home/tolian500/scripts/jdg_spolki_join/geckodriver"
    print("Geckodriver successfully founded")

    # Check if driver already exists, if not, install it
    if not os.path.exists(driver_path):
        driver_path = GeckoDriverManager().install()

    # Initialize service with the driver path


    service = FirefoxService(driver_path, port = port, log_output=os.path.join(BASE_DIR, "geckodriver.log"))


    # Set up Firefox options
    options = Options()
    options.set_preference("browser.download.folderList", 2)  # Use custom download directory
    options.set_preference("browser.download.dir", os.getcwd())  # Set download directory to current working directory
    options.set_preference("browser.download.panel.shown", False)  # Disable download panel
    options.set_preference("browser.download.useDownloadDir", True)  # Use the specified download directory
    options.set_preference("browser.helperApps.neverAsk.saveToDisk",
                           "application/pdf,text/csv,application/vnd.ms-excel")  # MIME types to automatically download
    options.set_preference("browser.helperApps.neverAsk.openFile",
                           "application/pdf,text/csv,application/vnd.ms-excel")  # MIME types to automatically open
    options.set_preference("pdfjs.disabled", True)  # Disable built-in PDF viewer

    # Optimisation options
    options.set_preference("permissions.default.image", 2)  # Block all images
    options.set_preference("browser.tabs.remote.autostart", False)  # Disable e10s/multiprocess
    options.set_preference("browser.tabs.remote.autostart.2", False)  # Disable e10s/multiprocess

    # Add headless option
    options.add_argument('--headless')
    print("Options for driver was set")

    # Initialize WebDriver
    try:
        driver = webdriver.Firefox(service=service, options=options)
    except Exception as e:
        print(f"Error while initialising driver: {e}")
    print("Driver should be created")
    return driver


def download_file(driver, curr_krs, cur_index: int):
    print("Start downloading")
    driver.get(URL)
    if cur_index == 0:
        # Chose przedsiembiorcy ONLY FOR FIRST TIME (Element)
        time.sleep(SHORT_PAGE_DELAY)
        driver.find_element(By.XPATH,
                            "/html/body/app-root/ds-layout/div/div/div/main/ds-layout-content/app-wyszukiwarka-krs/div/div[3]/ds-tab-view/div/div[1]/app-wyszukaj-podmiot/div/div[1]/ds-panel[1]/p-panel/div/div[2]/div/div/ds-panel[1]/p-panel/div/div[2]/div/div/div/ds-checkbox[1]/p-checkbox/label").click()

    # Enter krs to field:
    krs_input = driver.find_element(By.XPATH,
                                    "/html/body/app-root/ds-layout/div/div/div/main/ds-layout-content/app-wyszukiwarka-krs/div/div[3]/ds-tab-view/div/div[1]/app-wyszukaj-podmiot/div/div[1]/ds-panel[1]/p-panel/div/div[2]/div/div/ds-panel[2]/p-panel/div/div[2]/div/div/div/div[1]/div[1]/ds-input/div[1]/input")
    krs_input.clear()
    krs_input.send_keys(curr_krs)
    # print("Krs was entered")

    # Click search
    driver.find_element(By.XPATH,
                        '/html/body/app-root/ds-layout/div/div/div/main/ds-layout-content/app-wyszukiwarka-krs/div/div[3]/ds-tab-view/div/div[1]/app-wyszukaj-podmiot/div/div[2]/ds-panel/p-panel/div/div[2]/div/div/div/ds-button[2]/p-button/button').click()
    # print("Search was clicked")

    # Open  new page (firm page) by clicking on krs link
    time.sleep(SHORT_PAGE_DELAY)
    # print(f"CUrrent krs : {curr_krs}")
    element = driver.find_element(By.XPATH, f'//a[text()={curr_krs}]')
    # element = driver.find_element(By.LINK_TEXT, f'{curr_krs}')
    temp = element.location_once_scrolled_into_view
    time.sleep(LONG_PAGE_DELAY)
    element.click()

    time.sleep(SHORT_PAGE_DELAY)
    # Scroll view before downloading
    orient = driver.find_element(By.XPATH,
                                 "/html/body/app-root/ds-layout/div/div/div/main/ds-layout-content/app-dane-szczegolowe/div/div[4]/div[1]/ds-panel[2]/p-panel/div/div[2]/div/div/div/div[7]")
    temp = orient.location_once_scrolled_into_view
    time.sleep(SHORT_PAGE_DELAY)
    # Download file
    download_button = driver.find_element(By.XPATH,
                                          "/html/body/app-root/ds-layout/div/div/div/main/ds-layout-content/app-dane-szczegolowe/div/div[4]/div[2]/ds-panel/p-panel/div/div[2]/div/div/ds-panel[2]/p-panel/div/div[2]/div/div/div/ds-button[1]/p-button/button")
    download_button.click()

    # Wait for the file to finish downloading
    expected_filename = f"Odpis_Aktualny_KRS_{curr_krs}.pdf"  # Updated expected filename
    if wait_for_file_to_download(expected_filename, os.getcwd()):
        print(f"{curr_krs} was successfully downloaded")
    else:
        print(f"Failed to download {curr_krs}")


def wait_for_file_to_download(filename, download_dir):
    """
    Wait until the specified file is fully downloaded.
    """
    file_path = os.path.join(download_dir, filename)
    seconds = 0
    while not os.path.exists(file_path):
        time.sleep(1)
        seconds += 1
        if seconds > 60:  # Timeout after 60 seconds
            print(f"Timeout waiting for {filename} to be downloaded.")
            return False

    # Wait until the file is not a temporary file (like .part)
    while filename.endswith('.part'):
        time.sleep(1)
    return True


# MAIN func beginning


def main(krs_list: list):
    print("Download manager start")
    try:
        driver = initialize_driver()  # Initialize the driver once
        print("Driver sucecssfully initialized")
    except Exception as e:
        driver = None
        print(f"Driver error: {e}")

    if driver is not None:
        for element in krs_list:
            try:
                download_file(driver, element, krs_list.index(element))
            except Exception as e:
                print(e)
    # Wait for all downloads to complete before closing the driver

    driver.close()
    print("Driver was successfully closed")


if __name__ == "__main__":
    main(test_krs_list)
