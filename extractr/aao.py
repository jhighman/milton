import os
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from bs4 import BeautifulSoup
import time


# Constants
RUN_HEADLESS = True  # Set to False to run with the browser visible

# Define folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop2')
output_folder = os.path.join(folder_path, 'output2')
cache_folder = os.path.join(folder_path, 'cache2')

# Set up Chrome WebDriver options
chrome_options = Options()
if RUN_HEADLESS:
    chrome_options.add_argument("--headless")  # Run in headless mode
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

# Initialize Chrome service
service = ChromeService()  # Default ChromeDriver; add path if necessary

def handle_cookie_consent(driver):
    """
    Accept the cookie consent banner if it appears.
    """
    print("Step 1: Checking for cookie consent banner...")
    try:
        cookie_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Continue')]"))
        )
        driver.execute_script("arguments[0].click();", cookie_button)
        print("Step 2: Cookie consent accepted.")

        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element((By.XPATH, "//button[contains(text(), 'Continue')]"))
        )
        print("Step 3: Cookie banner dismissed.")
    except TimeoutException:
        print("Step 3: No cookie consent banner detected or already dismissed.")


def search_and_extract(first_name, last_name):
    """
    Perform a search on the FINRA website and extract the results table.
    
    Args:
        first_name (str): First name of the individual to search for.
        last_name (str): Last name of the individual to search for.

    Returns:
        dict: Search parameters and extracted table data.
    """
    print("Step 0: Initializing WebDriver...")
    # Use the Chrome options with headless toggle
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        print("Step 1: Navigating to the FINRA page...")
        driver.get("https://www.finra.org/rules-guidance/oversight-enforcement/finra-disciplinary-actions-online")

        # Handle the cookie consent banner
        handle_cookie_consent(driver)

        print("Step 4: Filling in the 'Individual Name or CRD#' field...")
        individuals_field = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "edit-individuals"))
        )
        individuals_field.clear()
        individuals_field.send_keys(f"{first_name} {last_name}")

        print("Step 5: Setting 'Document Types' to 'All Document Types'...")
        document_type_dropdown = Select(
            WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.ID, "edit-document-type"))
            )
        )
        document_type_dropdown.select_by_visible_text("All Document Types")

        print("Step 6: Clicking 'Terms of Service' checkbox...")
        terms_checkbox = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "edit-terms-of-service"))
        )
        if not terms_checkbox.is_selected():
            try:
                terms_checkbox.click()
                print("Step 6.1: 'Terms of Service' checkbox clicked.")
            except ElementClickInterceptedException:
                print("Step 6.2: 'Terms of Service' checkbox click intercepted. Using JavaScript.")
                driver.execute_script("arguments[0].click();", terms_checkbox)

        print("Step 7: Clicking the 'Submit' button...")
        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "edit-actions-submit"))
        )
        try:
            submit_button.click()
            print("Step 7.1: 'Submit' button clicked.")
        except ElementClickInterceptedException:
            print("Step 7.2: 'Submit' button click intercepted. Using JavaScript.")
            driver.execute_script("arguments[0].click();", submit_button)

        print("Step 8: Waiting for the results table to load...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.table-responsive.col > table.views-table.views-view-table.cols-5"))
        )
        print("Step 9: Results table loaded.")

        print("Step 10: Extracting table data...")
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find("table", class_="table views-table views-view-table cols-5")
        if not table:
            raise ValueError("No table found on the results page.")

        # Parse table headers
        headers = ["Case ID", "Case Summary", "Document Type", "Firms/Individuals", "Action Date"]

        # Parse table rows and count disciplinary actions
        rows = []
        disciplinary_action_count = 0
        for tr in table.find_all("tr")[1:]:  # Skip the header row
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            disciplinary_action = dict(zip(headers, cells))
            rows.append(disciplinary_action)
            disciplinary_action_count += 1

            # Print each disciplinary action as JSON
            print(json.dumps(disciplinary_action, indent=4))

        print(f"Total Disciplinary Actions Found: {disciplinary_action_count}")
        return {
            "search_parameters": {"first_name": first_name, "last_name": last_name},
            "results": rows,
            "disciplinary_action_count": disciplinary_action_count
        }

    except Exception as e:
        print(f"Error during search or extraction: {e}")
        return {"error": str(e)}

    finally:
        print("Step 12: Closing WebDriver...")
        driver.quit()


# Example usage
if __name__ == "__main__":
    print("Starting process...")
    result = search_and_extract("John", "Doe")
    print("Process completed. Results:")
    print(json.dumps(result, indent=4))
