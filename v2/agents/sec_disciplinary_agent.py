import os
from typing import Dict, Optional, Any, Generator
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from contextlib import contextmanager
import logging
from logging import Logger
import argparse
import json


"""
SEC Disciplinary Actions Online Search Tool


This script uses Selenium to search for disciplinary actions on the SEC's SALI page.
It follows the conventions of the FINRA disciplinary agent, focusing on individual searches.
Includes a main menu with a local test option for "Mark Miller".
"""


logger = logging.getLogger('sec_disciplinary_agent')


# Constants
RUN_HEADLESS = True  # Set to False to run with the browser visible


def create_driver(headless: bool = RUN_HEADLESS, logger: Logger = logger) -> webdriver.Chrome:
   """
   Create and configure a Chrome WebDriver.
   """
   logger.debug("Initializing Chrome WebDriver", extra={"headless": headless})
   options = Options()
   
   if headless:
       options.add_argument("--headless=new")
   
   # Required arguments for stable operation
   options.add_argument("--disable-gpu")
   options.add_argument("--no-sandbox")
   options.add_argument("--disable-dev-shm-usage")
   options.add_argument("--window-size=1920,1080")
   options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
   
   service = ChromeService()
   driver = webdriver.Chrome(service=service, options=options)
   
   return driver


@contextmanager
def get_driver(headless: bool = RUN_HEADLESS) -> Generator[webdriver.Chrome, None, None]:
   """
   Context manager for creating and cleaning up a Chrome WebDriver.
  
   Args:
       headless: Whether to run browser in headless mode
      
   Yields:
       A configured Chrome WebDriver instance
   """
   driver = create_driver(headless)
   try:
       yield driver
   finally:
       driver.quit()


def search_individual(driver: webdriver.Chrome, first_name: str, last_name: str, 
                     logger: Logger = logger) -> Dict[str, Any]:
    """
    Search for an individual's SEC disciplinary actions.

    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance.
        first_name (str): First name to search.
        last_name (str): Last name to search.
        logger (Logger): Logger instance for structured logging.

    Returns:
        Dict[str, Any]: Dictionary containing search results and individual info
    """
    logger.info("Searching SEC disciplinary actions", extra={"first_name": first_name, "last_name": last_name})
    
    try:
        # Navigate to SEC SALI page
        logger.debug("Navigating to SEC SALI page")
        driver.get("https://www.sec.gov/litigations/sec-action-look-up")
        
        # Wait for form to load
        logger.debug("Waiting for search form")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "edit-last-name"))
        )

        # Fill in search fields
        logger.debug("Filling in search fields")
        last_name_input = driver.find_element(By.ID, "edit-last-name")
        last_name_input.clear()
        last_name_input.send_keys(last_name)

        first_name_input = driver.find_element(By.ID, "edit-first-name")
        first_name_input.clear()
        if first_name and isinstance(first_name, str) and first_name.strip():
            first_name_input.send_keys(first_name)

        # Submit the form
        logger.debug("Submitting form")
        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "edit-submit-bad-actors"))
        )
        driver.execute_script("arguments[0].click();", submit_button)

        # Wait for results or no-results message
        logger.debug("Waiting for search results")
        WebDriverWait(driver, 20).until(
            lambda driver: driver.find_elements(By.CLASS_NAME, "card") or
                           driver.find_elements(By.CLASS_NAME, "view-empty")
        )

        # Parse results
        logger.debug("Parsing search results")
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        cards = soup.find_all("div", class_="card border-divide views-row")

        if not cards:
            logger.info("No results found for search")
            return {"result": "No Results Found"}

        result_rows = []
        for card in cards:
            # Extract fields from the card
            name = card.find("h2", class_="field-content card-title usa-collection__heading")
            name_text = name.get_text(strip=True) if name else "Unknown"

            aka = card.find("div", class_="views-field-field-also-known-as-1")
            aka_text = aka.find("span", class_="field-content").get_text(strip=True) if aka else ""

            age = card.find("div", class_="views-field-field-age-in-document")
            age_text = age.find("span", class_="field-content").get_text(strip=True) if age else ""

            state = card.find("div", class_="views-field-field-state-idd")
            state_text = state.find("span", class_="field-content").get_text(strip=True) if state else ""

            action = card.find("div", class_="views-field-field-action-name-in-document")
            action_text = action.find("span", class_="field-content").get_text(strip=True) if action else "Unknown"

            date_filed = card.find("div", class_="views-field-field-date-filed")
            date_text = date_filed.find("time", class_="datetime").get_text(strip=True) if date_filed else "Unknown"

            documents_div = card.find("div", class_="views-field-field-related-documents")
            documents = []
            if documents_div:
                doc_items = documents_div.find("div", class_="item-list")
                if doc_items:
                    for li in doc_items.find_all("li"):
                        doc_link = li.find("a")
                        doc_title = doc_link.get_text(strip=True) if doc_link else ""
                        doc_url = doc_link.get("href") if doc_link else ""
                        doc_date = li.find("time", class_="datetime")
                        doc_date_text = doc_date.get_text(strip=True) if doc_date else ""
                        if doc_title and doc_url:
                            documents.append({
                                "title": doc_title,
                                "link": doc_url,
                                "date": doc_date_text
                            })
                            logger.debug("Found document", extra={"title": doc_title, "link": doc_url, "date": doc_date_text})

            result_rows.append({
                "Name": name_text,
                "Also Known As": aka_text,
                "Current Age": age_text,
                "State": state_text,
                "Enforcement Action": action_text,
                "Date Filed": date_text,
                "Documents": documents
            })

        logger.info("Search completed successfully", extra={"result_count": len(result_rows)})
        return {"result": result_rows}

    except TimeoutException as e:
        logger.error("Timeout during search process", extra={"error": str(e)})
        return {"error": f"Timeout: {str(e)}"}
    except WebDriverException as e:
        logger.error("WebDriver error during search", extra={"error": str(e)})
        return {"error": f"WebDriver error: {str(e)}"}
    except Exception as e:
        logger.error("Unexpected error in search process", extra={"error": str(e)})
        return {"error": str(e)}


def main() -> None:
   parser = argparse.ArgumentParser(description='Search SEC disciplinary actions')
   parser.add_argument('--first-name', help='First name to search')
   parser.add_argument('--last-name', help='Last name to search')
   parser.add_argument('--headless', action='store_true', default=RUN_HEADLESS, help='Run in headless mode')
  
   args = parser.parse_args()


   def run_search(first_name: str, last_name: str, headless: bool):
       with get_driver(headless) as driver:
           result = search_individual(driver, first_name, last_name, logger=logger)
           print(json.dumps(result, indent=2))


   if args.first_name or args.last_name:
       # Command-line search
       if not args.last_name:
           print("Error: --last-name is required for custom searches.")
           parser.print_help()
           return
       run_search(args.first_name or "", args.last_name, args.headless)
   else:
       # Interactive menu
       while True:
           print("\nSEC Disciplinary Actions Search Menu:")
           print("1. Run local test with 'Mark Miller'")
           print("2. Perform custom search")
           print("3. Exit")
           choice = input("Enter your choice (1-3): ").strip()


           if choice == "1":
               print("\nRunning local test with 'Mark Miller'...")
               run_search("Mark", "Miller", RUN_HEADLESS)
           elif choice == "2":
               first_name = input("Enter first name (optional, press Enter to skip): ").strip()
               last_name = input("Enter last name (required): ").strip()
               if not last_name:
                   print("Error: Last name is required.")
                   continue
               run_search(first_name, last_name, RUN_HEADLESS)
           elif choice == "3":
               print("Exiting...")
               break
           else:
               print("Invalid choice. Please enter 1, 2, or 3.")


if __name__ == "__main__":
   main()

