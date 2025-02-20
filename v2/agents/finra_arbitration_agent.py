import os
import json
import argparse
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

"""
FINRA Arbitration Awards Search Tool with Robust Result Processing
"""

# Constants
RUN_HEADLESS = True
input_folder = './drop'
output_folder = './output'
cache_folder = './cache'

def create_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=ChromeService(), options=options)

def handle_cookie_consent(driver):
    """Dismiss the cookie consent popup with 3-second delays"""
    try:
        print("Waiting 3 seconds for page to load and consent popup to appear...")
        time.sleep(3)
        print("Locating cookie consent 'Continue' button...")
        consent_button = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".cc-btn.cc-dismiss"))
        )
        driver.execute_script("arguments[0].click();", consent_button)
        print("Cookie consent popup dismissed")
        print("Waiting 3 seconds after dismissing consent...")
        time.sleep(1)
    except TimeoutException:
        print("No cookie consent popup appeared within 10 seconds")
    except Exception as e:
        print(f"Error dismissing cookie consent: {e}")

def search_individual(driver, first_name, last_name):
    search_term = f"{first_name} {last_name}"
    if not first_name or not last_name:
        raise ValueError("Both first_name and last_name are required")
    return process_arbitration_search(driver, search_term)

def process_arbitration_search(driver, search_term):
    print(f"Starting search for '{search_term}'...")
    try:
        print("Navigating to Arbitration Awards page...")
        driver.get("https://www.finra.org/arbitration-mediation/arbitration-awards")
        
        # Step 1: Handle cookie consent popup
        handle_cookie_consent(driver)

        # Step 2: Click Terms of Service checkbox
        print("Locating Terms of Service checkbox...")
        terms_checkbox = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "edit-terms-of-service"))
        )
        if not terms_checkbox.is_selected():
            driver.execute_script("arguments[0].click();", terms_checkbox)
            print("Clicked Terms of Service checkbox")
            print("Waiting 5 seconds after clicking Terms of Service...")
            time.sleep(5)
        else:
            print("Terms of Service checkbox already selected")

        # Step 3: Enter search term
        print("Locating search input...")
        search_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "edit-search"))
        )
        search_input.clear()
        time.sleep(1)
        search_input.send_keys(search_term)
        print(f"Entered search term: '{search_term}'")
        time.sleep(2)

        # Step 4: Submit the form
        print("Locating and clicking search button...")
        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "edit-actions-submit"))
        )
        driver.execute_script("arguments[0].click();", submit_button)
        print("Waiting 5 seconds after submitting search...")
        time.sleep(5)

        # Step 5: Wait for results
        print("Waiting for results...")
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//em[text()='No Results Found']"))
            )
            print("No results found")
            return {"result": "No Results Found"}
        except:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.views-table.views-view-table.cols-5"))
            )
            print("Results table detected")
            time.sleep(3)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find("table", class_=["views-table", "views-view-table", "cols-5"])
            
            if not table:
                print("No results table found in BeautifulSoup parse")
                print(f"Page source length: {len(driver.page_source)}")
                driver.save_screenshot("debug_table_not_found.png")
                # Fallback to Selenium extraction
                table_element = driver.find_element(By.CSS_SELECTOR, "table.views-table.views-view-table.cols-5")
                soup = BeautifulSoup(table_element.get_attribute("outerHTML"), 'html.parser')
                table = soup.find("table")
                if not table:
                    print("Fallback failed: Table still not found")
                    return {"result": "No Results Found"}
                print("Fallback to Selenium table extraction succeeded")

            print("Table found, parsing results...")
            headers = ["Award Document", "Case Summary", "Document Type", "Forum", "Date of Award"]
            result_rows = []
            tbody = table.find("tbody")
            if not tbody:
                print("No tbody found in table")
                return {"result": "No Results Found"}
            
            for tr in tbody.find_all("tr"):
                row = {}
                cells = tr.find_all("td")
                for header, td in zip(headers, cells):
                    if header == "Award Document":
                        link = td.find("a")
                        row[header] = link.get_text(strip=True) if link else td.get_text(strip=True)
                        row["PDF URL"] = link["href"] if link else ""
                    elif header == "Case Summary":
                        details = {}
                        for div in td.find_all("div", recursive=True):
                            if div.find("span", class_="spanBold"):
                                label = div.find("span", class_="spanBold").get_text(strip=True)
                                value = div.get_text(strip=True).replace(label, "").strip()
                                details[label] = value
                        row[header] = details
                    else:
                        row[header] = td.get_text(strip=True)
                result_rows.append(row)
            
            print(f"Extracted {len(result_rows)} awards")
            return {"result": result_rows}

    except Exception as e:
        print(f"Error during search: {e}")
        driver.save_screenshot("debug_error.png")
        return {"error": str(e)}

def validate_json_data(data, file_path):
    if "claim" not in data or not all(k in data["claim"] for k in ["first_name", "last_name"]):
        return False, f"Missing or invalid 'claim' in {file_path}"
    if "alternate_names" in data and not all(isinstance(pair, list) and len(pair) == 2 for pair in data["alternate_names"]):
        return False, f"Invalid 'alternate_names' in {file_path}"
    return True, ""

def search_with_alternates(driver, first_name, last_name, alternate_names=None):
    all_names = [(first_name, last_name)] + (alternate_names or [])
    return [search_individual(driver, fname, lname) for fname, lname in all_names]

def batch_process_folder():
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)
    
    stats = {'total_searches': 0, 'awards_found': 0, 'no_results': 0, 'errors': 0}
    
    with create_driver(RUN_HEADLESS) as driver:
        for json_file in [f for f in os.listdir(input_folder) if f.endswith('.json')]:
            file_path = os.path.join(input_folder, json_file)
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                is_valid, error = validate_json_data(data, file_path)
                if not is_valid:
                    stats['errors'] += 1
                    print(f"Validation error: {error}")
                    continue

                claim = data["claim"]
                results = search_with_alternates(
                    driver,
                    claim["first_name"],
                    claim["last_name"],
                    data.get("alternate_names", [])
                )
                
                stats['total_searches'] += len(results)
                handle_search_results(results, claim["first_name"], claim["last_name"], stats)

            except Exception as e:
                stats['errors'] += 1
                print(f"Error processing {file_path}: {e}")
    
    print(f"Summary: Searches={stats['total_searches']}, Awards={stats['awards_found']}, No Results={stats['no_results']}, Errors={stats['errors']}")
    return stats

def handle_search_results(results, first_name, last_name, stats):
    for result in results:
        if result.get("result") == "No Results Found":
            stats['no_results'] += 1
        elif "error" in result:
            stats['errors'] += 1
        else:
            stats['awards_found'] += len(result["result"])
    
    output_path = os.path.join(output_folder, f"{first_name}_{last_name}.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=4)
    print(f"Saved results to {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Search FINRA Arbitration Awards')
    parser.add_argument('--first-name', help='First name to search')
    parser.add_argument('--last-name', help='Last name to search')
    parser.add_argument('--batch', action='store_true', help='Process JSON files in drop folder')
    parser.add_argument('--headless', action='store_true', default=RUN_HEADLESS, help='Run in headless mode')
    
    args = parser.parse_args()
    
    if args.first_name and args.last_name:
        with create_driver(args.headless) as driver:
            result = search_individual(driver, args.first_name, args.last_name)
            print(json.dumps(result, indent=2))
    elif args.batch:
        batch_process_folder()
    else:
        print("Please provide --first-name and --last-name, or use --batch.")

if __name__ == "__main__":
    main()