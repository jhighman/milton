import requests
import csv
import time
import random  # Import random module for generating random increments and sleep times

# Toggle to enable or disable the disclosure filter
filter_disclosures = False  # Set to False to disable the filter

# Headers to include in the requests
headers = {

}

def get_individual_basic_info(crd_number):
    url = 'https://api.brokercheck.finra.org/search/individual'
    
    params = {
        'query': crd_number,
        'filter': 'active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true',
        'includePrevious': 'true',
        'hl': 'true',
        'nrows': '12',
        'start': '0',
        'r': '25',
        'sort': 'score+desc',
        'wt': 'json'
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 403:
        print(f"403 Forbidden Error fetching basic info for CRD {crd_number}")
        try:
            error_content = response.json()
            error_message = error_content.get('message', response.text)
        except ValueError:
            error_message = response.text
        print(f"Error message: {error_message}")
        return None
    else:
        print(f"Error fetching basic info for CRD {crd_number}: {response.status_code}")
        return None

def get_individual_detailed_info(crd_number):
    url = f'https://api.brokercheck.finra.org/individual/summary/{crd_number}'
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 403:
        print(f"403 Forbidden Error fetching detailed info for CRD {crd_number}")
        try:
            error_content = response.json()
            error_message = error_content.get('message', response.text)
        except ValueError:
            error_message = response.text
        print(f"Error message: {error_message}")
        return None
    else:
        print(f"Error fetching detailed info for CRD {crd_number}: {response.status_code}")
        return None

def derive_license_type(bc_scope: str, ia_scope: str) -> str:
    bc_active = bc_scope.lower() == 'active'
    ia_active = ia_scope.lower() == 'active'
    
    if bc_active and ia_active:
        return 'B IA'
    elif bc_active:
        return 'B'
    elif ia_active:
        return 'IA'
    else:
        return ''

def process_crd_numbers(seed_crd: int, iterations: int, output_csv_path: str):
    # Open the file in append mode ('a') and skip writing headers if the file exists
    with open(output_csv_path, 'a', newline='') as csvfile_out:
        fieldnames = ['crd_number', 'last_name', 'first_name', 'license_type']
        csv_writer = csv.DictWriter(csvfile_out, fieldnames=fieldnames)
        
        # If the file is empty, write the header
        if csvfile_out.tell() == 0:
            csv_writer.writeheader()
        
        current_crd = seed_crd
        for _ in range(iterations):
            crd_number = str(current_crd)
            basic_info = get_individual_basic_info(crd_number)
            if basic_info and basic_info.get('hits', {}).get('hits'):
                individual = basic_info['hits']['hits'][0].get('_source', {})
                first_name = individual.get('ind_firstname', '').strip()
                middle_name = individual.get('ind_middlename', '').strip()
                last_name = individual.get('ind_lastname', '').strip()
                bc_scope = individual.get('ind_bc_scope', '')
                ia_scope = individual.get('ind_ia_scope', '')
                
                # Handle cases where middle name is present
                if middle_name:
                    first_name_full = f"{first_name} {middle_name}"
                else:
                    first_name_full = first_name
                
                # Fetch detailed info to check disclosures
                detailed_info = get_individual_detailed_info(crd_number)
                if detailed_info:
                    disclosures = detailed_info.get('disclosures', [])
                else:
                    disclosures = []
                
                # Apply the disclosure filter
                if filter_disclosures:
                    if not disclosures:
                        print(f"CRD {crd_number}: No disclosures found, skipping.")
                        # Increment current_crd before continuing
                        current_crd += random.randint(3, 203)
                        # Random sleep time between 2 and 7 seconds
                        sleep_time = random.uniform(2, 5)
                        time.sleep(sleep_time)
                        continue  # Skip this CRD number
                
                # Derive license type
                license_type = derive_license_type(bc_scope, ia_scope)
                
                # Write to output CSV
                csv_writer.writerow({
                    'crd_number': crd_number,
                    'last_name': last_name,
                    'first_name': first_name_full,
                    'license_type': license_type
                })
                
                print(f"Processed CRD {crd_number}: {last_name}, {first_name_full}, License Type: {license_type}")
            else:
                print(f"No basic info found for CRD {crd_number}, skipping.")
            
            # Increment current_crd by a random number between 3 and 150
            increment = random.randint(3, 150)
            current_crd += increment

            # Random sleep time between 2 and 7 seconds
            sleep_time = random.uniform(2, 7)
            time.sleep(sleep_time)
    
    print(f"\nResults have been written to {output_csv_path}")

if __name__ == "__main__":
    seed_crd = 6135614    # Starting CRD number
    iterations = 20       # Number of iterations
    output_csv_path = 'output.csv'    # Output CSV file path
    process_crd_numbers(seed_crd, iterations, output_csv_path)
