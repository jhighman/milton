import os
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from time import sleep

# Define folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop2')
output_folder = os.path.join(folder_path, 'output2')
cache_folder = os.path.join(folder_path, 'cache2')

# Global counters for summary
no_enforcement_action_count = 0
enforcement_action_alerts_count = 0
skipped_files_count = 0
individuals_searched_count = 0
total_searches_count = 0
errors_count = 0
skipped_files = []

# Set up Chrome WebDriver options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run headless
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

def generate_sec_search_url(first_name, last_name):
    """Generates the SEC search URL using first and last names."""
    base_url = "https://www.sec.gov/litigations/sec-action-look-up?last_name={}&first_name={}"
    return base_url.format(last_name, first_name)

def fetch_and_parse_sec(driver, first_name, last_name):
    """
    Fetches and parses SEC data using Selenium WebDriver.

    Parameters:
        driver: Selenium WebDriver instance.
        first_name (str): First name of the individual.
        last_name (str): Last name of the individual.

    Returns:
        dict: Parsed SEC data or an error message.
    """
    global total_searches_count, no_enforcement_action_count, enforcement_action_alerts_count
    total_searches_count += 1
    search_url = generate_sec_search_url(first_name, last_name)

    try:
        driver.get(search_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "main-content"))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Check for "No Results"
        no_results = soup.find("p", class_="no-results")
        if no_results:
            no_enforcement_action_count += 1
            return {"first_name": first_name, "last_name": last_name, "result": "No Results Found"}

        # Parse results if available
        results = soup.find_all("div", class_="card border-divide views-row")
        if not results:
            no_enforcement_action_count += 1
            return {"first_name": first_name, "last_name": last_name, "result": "No Results Found"}

        enforcement_action_alerts_count += 1
        data = []
        for result in results:
            name = result.find("h2", class_="field-content card-title").get_text(strip=True)
            state = result.find("span", class_="field-content").get_text(strip=True)
            action = result.find("span", class_="field-content", string="Enforcement Action:").find_next_sibling().get_text(strip=True)
            date_filed = result.find("time", class_="datetime").get_text(strip=True)
            documents = []

            related_documents = result.find_all("div", class_="field__item")
            for doc in related_documents:
                title = doc.find("a").get_text(strip=True)
                link = doc.find("a")["href"]
                documents.append({"title": title, "link": f"https://www.sec.gov{link}"})

            data.append({
                "Name": name,
                "State": state,
                "Enforcement Action": action,
                "Date Filed": date_filed,
                "Documents": documents
            })

        return {"first_name": first_name, "last_name": last_name, "result": data}

    except Exception as e:
        return {"first_name": first_name, "last_name": last_name, "error": str(e)}

def extract_first_and_last(full_name):
    """
    Extracts first and last names from a full name string, omitting middle names.
    """
    parts = full_name.split()
    if len(parts) > 2:
        return parts[0], parts[-1]
    elif len(parts) == 2:
        return parts[0], parts[1]
    return full_name, ""

def process_json_file(driver, file_path):
    """
    Processes a JSON file, extracts data, and writes to cache if valid.
    """
    global skipped_files_count, skipped_files, individuals_searched_count, errors_count

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        claim = data.get("claim", {})
        full_name = claim.get("name")
        employee_number = claim.get("employee_number", "unknown")
        ind_other_names = data.get("search_evaluation", {}).get("individual", {}).get("ind_other_names", [])

        if not full_name or not employee_number:
            skipped_files_count += 1
            skipped_files.append(file_path)
            return

        individuals_searched_count += 1

        # Handle name variations (alternate names)
        all_names = [full_name] + ind_other_names
        all_name_variations = [extract_first_and_last(name) for name in all_names]

        employee_dir = os.path.join(cache_folder, employee_number)
        os.makedirs(employee_dir, exist_ok=True)

        for idx, (first_name, last_name) in enumerate(all_name_variations, start=1):
            if not first_name or not last_name:
                continue

            search_result = fetch_and_parse_sec(driver, first_name, last_name)
            output_file = os.path.join(employee_dir, f"sec_search_{idx}.json")

            with open(output_file, 'w', encoding='utf-8') as outfile:
                json.dump(search_result, outfile, indent=4)

        print(f"Processed and cached: {employee_number} ({len(all_name_variations)} searches)")

    except Exception as e:
        errors_count += 1
        print(f"Error processing file {file_path}: {e}")

def summarize_results():
    """Prints the summary of search results."""
    print("\n--- Summary of Results ---")
    print(f"Individuals Searched: {individuals_searched_count}")
    print(f"Total Searches: {total_searches_count}")
    print(f"No Enforcement Actions: {no_enforcement_action_count}")
    print(f"Enforcement Action Alerts: {enforcement_action_alerts_count}")
    print(f"Skipped Files: {skipped_files_count}")
    print(f"Errors: {errors_count}")
    if skipped_files:
        print("\nList of Skipped Files:")
        for file in skipped_files:
            print(f"- {file}")

def main():
    """Main function."""
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)

    driver = webdriver.Chrome(service=ChromeService(), options=chrome_options)
    try:
        json_files = [f for f in os.listdir(input_folder) if f.endswith('.json')]
        if not json_files:
            print("No .json files found in the input folder.")
            return

        for json_file in json_files:
            file_path = os.path.join(input_folder, json_file)
            process_json_file(driver, file_path)

    finally:
        driver.quit()

    summarize_results()

if __name__ == "__main__":
    main()
