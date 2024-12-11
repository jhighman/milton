import os
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# Define folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop2')
output_folder = os.path.join(folder_path, 'output2')
cache_folder = os.path.join(folder_path, 'cache2')

# Set up Chrome WebDriver options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run headless
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

service = ChromeService()

# Global counters for summary
no_enforcement_action_count = 0
enforcement_action_alerts_count = 0
skipped_files_count = 0
individuals_searched_count = 0
total_searches_count = 0
errors_count = 0
skipped_files = []

def generate_sec_search_url(first_name, last_name):
    """
    Generates the search URL using first and last names.
    """
    base_url = "https://www.sec.gov/litigations/sec-action-look-up?last_name={}&first_name={}"
    return base_url.format(last_name, first_name)

def fetch_and_parse_sec(driver, input_data):
    """
    Uses Selenium WebDriver to fetch and parse the SEC Action Lookup page.
    """
    global no_enforcement_action_count, enforcement_action_alerts_count, total_searches_count

    search_url = input_data.get("search")
    first_name = input_data.get("first_name", "Unknown")
    last_name = input_data.get("last_name", "Unknown")

    try:
        # Increment total searches
        total_searches_count += 1

        # Load the search page
        driver.get(search_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "main-content"))
        )

        # Parse the page content
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        # Check for "No Results" message
        no_results_element = soup.find("p", class_="no-results")
        if no_results_element:
            no_enforcement_action_count += 1
            return {
                "first_name": first_name,
                "last_name": last_name,
                "result": "No Results Found"
            }

        # Parse results if available
        results = soup.find_all("div", class_="card border-divide views-row")
        if not results:
            no_enforcement_action_count += 1
            return {
                "first_name": first_name,
                "last_name": last_name,
                "result": "No Results Found"
            }

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

        return {
            "first_name": first_name,
            "last_name": last_name,
            "result": data
        }

    except Exception as e:
        global errors_count
        errors_count += 1
        return {
            "first_name": first_name,
            "last_name": last_name,
            "error": str(e)
        }

def process_json_file(driver, file_path):
    """
    Processes a JSON file, extracts data, and writes results to cache and output if enforcement actions exist.
    """
    global skipped_files_count, skipped_files, individuals_searched_count

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Extract claim data
        claim = data.get("claim", {})
        first_name = claim.get("first_name")
        last_name = claim.get("last_name")
        employee_number = claim.get("employee_number", "unknown")

        # Extract alternate names
        alternate_names = data.get("search_evaluation", {}).get("individual", {}).get("ind_other_names", [])

        if not first_name or not last_name:
            print(f"Missing 'first_name' or 'last_name' in `claim` object for file: {file_path}")
            skipped_files_count += 1
            skipped_files.append(file_path)
            return

        # Increment individuals searched
        individuals_searched_count += 1

        # Create list of all name variations
        all_names = [(first_name, last_name)] + [
            (name.split()[0], name.split()[-1]) for name in alternate_names
        ]

        # Process each name variation
        for idx, (alt_first_name, alt_last_name) in enumerate(all_names, start=1):
            search_url = generate_sec_search_url(alt_first_name, alt_last_name)
            search_data = {"first_name": alt_first_name, "last_name": alt_last_name, "search": search_url}
            result = fetch_and_parse_sec(driver, search_data)

            # Write to cache folder
            employee_dir_cache = os.path.join(cache_folder, employee_number)
            os.makedirs(employee_dir_cache, exist_ok=True)
            output_file_cache = os.path.join(employee_dir_cache, f"sec_result_{idx}.json")
            with open(output_file_cache, 'w', encoding='utf-8') as outfile:
                json.dump(result, outfile, indent=4)

            # Write to output folder if enforcement actions are present
            if isinstance(result.get("result"), list) and result["result"]:
                employee_dir_output = os.path.join(output_folder, employee_number)
                os.makedirs(employee_dir_output, exist_ok=True)
                output_file_output = os.path.join(employee_dir_output, f"sec_result_{idx}.json")
                with open(output_file_output, 'w', encoding='utf-8') as outfile:
                    json.dump(result, outfile, indent=4)

        print(f"Processed and cached: {employee_number} ({len(all_names)} names searched)")

    except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
        print(f"Error processing file {file_path}: {e}")
        skipped_files_count += 1
        skipped_files.append(file_path)

def summarize_results():
    """
    Prints the summary of enforcement results.
    """
    print("\n--- Summary of Results ---")
    print(f"Individuals Searched: {individuals_searched_count}")
    print(f"Total Searches: {total_searches_count}")
    print(f"No Enforcement Actions: {no_enforcement_action_count}")
    print(f"Enforcement Action Alerts: {enforcement_action_alerts_count}")
    print(f"Skipped Files: {skipped_files_count}")
    print(f"Errors Encountered: {errors_count}")
    if skipped_files:
        print("\nList of Skipped Files:")
        for file in skipped_files:
            print(f"- {file}")

def main():
    """
    Main function to process all JSON files and print a summary.
    """
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(cache_folder, exist_ok=True)

    # Initialize WebDriver
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # Scan the input folder for JSON files
        json_files = [f for f in os.listdir(input_folder) if f.endswith('.json')]
        if not json_files:
            print("No .json files found in the input folder.")
            return

        # Process each JSON file
        for json_file in json_files:
            file_path = os.path.join(input_folder, json_file)
            process_json_file(driver, file_path)

        # Summarize results
        summarize_results()
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
