import os
import json

# Define folder paths
folder_path = './'
input_folder = os.path.join(folder_path, 'drop')
output_folder = os.path.join(folder_path, 'output')
archive_folder = os.path.join(folder_path, 'archive')
cache_folder = os.path.join(folder_path, 'cache')
config_file = os.path.join(folder_path, 'config.json')

# Default configuration to be written to config.json if it doesn't exist
default_config = {
    "evaluate_name": True,
    "evaluate_license": True,
    "evaluate_exams": True,
    "evaluuate_disclosures": True
}

# Function to create directories if they don't exist
def create_folders():
    directories = [input_folder, output_folder, archive_folder, cache_folder]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        else:
            print(f"Directory already exists: {directory}")

# Function to create config.json if it doesn't exist
def create_config():
    if not os.path.exists(config_file):
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=4)
        print(f"Created config file with default settings at: {config_file}")
    else:
        print(f"Config file already exists at: {config_file}")

if __name__ == "__main__":
    # Create folders
    create_folders()
    
    # Create config.json
    create_config()

    print("Setup complete!")
