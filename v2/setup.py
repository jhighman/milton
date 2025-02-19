import os
import json

def create_folders():
    """Create required directories if they don't exist"""
    directories = ['drop', 'output', 'archive', 'cache', 'index']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        else:
            print(f"Directory already exists: {directory}")

def create_config():
    """Create config.json with default settings if it doesn't exist"""
    config_file = 'config.json'
    default_config = {
        "evaluate_name": True,
        "evaluate_license": True,
        "evaluate_exams": True,
        "evaluate_disclosures": True  # Fixed typo
    }
    
    if not os.path.exists(config_file):
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=4)
        print(f"Created config file with default settings at: {config_file}")
    else:
        print(f"Config file already exists at: {config_file}")

def main():
    # Ensure we're in the v2 directory
    if not os.path.basename(os.getcwd()) == 'v2':
        os.chdir('v2')
        print("Changed working directory to v2/")
    
    # Create folders and config
    create_folders()
    create_config()
    
    print("\nSetup complete!")
    print("Run 'python verify_setup.py' to verify the installation.")

if __name__ == "__main__":
    main()
