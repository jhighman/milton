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
NFA BASIC Search Tool Agent for Individuals
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

def search_nfa_profile(driver, first_name, last_name):
    search_term = f"{first_name} {last_name}"
    print(f"Starting search for '{search_term}'...")
    try:
        print("Navigating to NFA BASIC search page...")
        driver.get("https://www.nfa.futures.org/BasicNet/#profile")
        
        # Step 1: Wait for page load
        print("Waiting 2 seconds for page to load...")
        time.sleep(2)

        # Step 2: Select Individual tab (if not active)
        print("Locating Individual tab...")
        individual_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//ul[@id='landing_search_tabs']//a[contains(text(), 'Individual')]"))
        )
        if "active" not in individual_tab.find_element(By.XPATH, "..").get_attribute("class"):
            driver.execute_script("arguments[0].click();", individual_tab)
            print("Clicked Individual tab")
            time.sleep(0.5)
        else:
            print("Individual tab already active")

        # Step 3: Enter search terms
        print("Locating first name input...")
        fname_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "fname_in_lsearch_tabs"))
        )
        fname_input.clear()
        fname_input.send_keys(first_name)
        print(f"Entered first name: '{first_name}'")

        print("Locating last name input...")
        lname_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@id='individual']//input[@placeholder='Last Name (required)']"))
        )
        lname_input.clear()
        lname_input.send_keys(last_name)
        print(f"Entered last name: '{last_name}'")

        # Step 4: Submit the form
        print("Locating and clicking search button...")
        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@id='individual']//button[contains(text(), 'Search')]"))
        )
        driver.execute_script("arguments[0].click();", submit_button)
        print("Waiting 5 seconds after submitting search...")
        time.sleep(5)

        # Step 5: Wait for results
        print("Waiting for results...")
        
        try:
            # First check if we can find either results table or no results message
            results_table = driver.find_element(By.ID, "table_individual_name_results")
            print("Found results table")
            time.sleep(1)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find("table", id="table_individual_name_results")
            
            if not table or not table.find("tbody"):
                print("No results table or tbody found")
                print(f"Table found: {bool(table)}")
                print(f"Table HTML: {table}")
                driver.save_screenshot("debug_table_not_found.png")
                return {"result": "No Results Found"}

            print("Table found, parsing results...")
            headers = ["Individual Name", "Current NFA Membership Status", "Current Registration Types", "Regulatory Actions"]
            result_rows = []
            for tr in table.find("tbody").find_all("tr"):
                row = {}
                cells = tr.find_all("td")
                name_cell = cells[0]
                name = name_cell.find("h4").get_text(strip=True) if name_cell.find("h4") else ""
                small_text = name_cell.find("small").get_text(strip=True) if name_cell.find("small") else ""
                nfa_id, firm = small_text.split(" | ", 1) if " | " in small_text else (small_text, "")
                
                row["Name"] = name
                row["NFA ID"] = nfa_id
                row["Firm"] = firm
                
                for header, td in zip(headers[1:], cells[1:4]):  # Skip name cell, stop before "View Details"
                    row[header] = td.find("span").get_text(strip=True) if td.find("span") else td.get_text(strip=True)
                
                row["Details Available"] = "Yes" if cells[-1].find("div", class_="btn") else "No"
                result_rows.append(row)
            
            print(f"Extracted {len(result_rows)} profiles")
            # Check if we found any results
            if not result_rows:
                return {"result": "No Results Found"}
            return {"result": result_rows}

        except:
            try:
                no_results = driver.find_element(By.ID, "basic_search_no_results")
                print("No results element found with text:", no_results.text)
                return {"result": "No Results Found"}
            except:
                print("Neither results table nor 'no results' message found")
                driver.save_screenshot("debug_no_elements_found.png")
                return {"error": "Could not detect results or no-results message"}

    except Exception as e:
        print(f"Error during search: {e}")
        driver.save_screenshot("debug_error.png")
        return {"error": str(e)}

def validate_json_data(data, file_path):
    if "claim" not in data or not all(k in data["claim"] for k in ["first_name", "last_name"]):
        return False, f"Missing or invalid 'claim' in {file_path}"
    return True, ""

def search_with_alternates(driver, first_name, last_name, alternate_names=None):
    all_names = [(first_name, last_name)] + (alternate_names or [])
    return [search_nfa_profile(driver, fname, lname) for fname, lname in all_names]

def batch_process_folder():
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)
    
    stats = {'total_searches': 0, 'profiles_found': 0, 'no_results': 0, 'errors': 0}
    
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
                handle_search_results(results, f"{claim['first_name']}_{claim['last_name']}", stats)

            except Exception as e:
                stats['errors'] += 1
                print(f"Error processing {file_path}: {e}")
    
    print(f"Summary: Searches={stats['total_searches']}, Profiles={stats['profiles_found']}, No Results={stats['no_results']}, Errors={stats['errors']}")
    return stats

def handle_search_results(results, output_name, stats):
    for result in results:
        if result.get("result") == "No Results Found":
            stats['no_results'] += 1
        elif "error" in result:
            stats['errors'] += 1
        else:
            stats['profiles_found'] += len(result["result"])
    
    output_path = os.path.join(output_folder, f"{output_name}.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=4)
    print(f"Saved results to {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Search NFA BASIC Individual Profiles')
    parser.add_argument('--first-name', help='First name to search')
    parser.add_argument('--last-name', help='Last name to search')
    parser.add_argument('--batch', action='store_true', help='Process JSON files in drop folder')
    parser.add_argument('--headless', action='store_true', default=RUN_HEADLESS, help='Run in headless mode')
    
    args = parser.parse_args()
    
    if args.first_name and args.last_name:
        with create_driver(args.headless) as driver:
            result = search_nfa_profile(driver, args.first_name, args.last_name)
            print(json.dumps(result, indent=2))
    elif args.batch:
        batch_process_folder()
    else:
        print("Please provide --first-name and --last-name, or use --batch.")

if __name__ == "__main__":
    main()