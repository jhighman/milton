import os
import json
from setuptools import setup, find_packages  # type: ignore

def create_folders():
    """Create required directories if they don't exist"""
    directories = ['drop', 'output', 'archive', 'cache', 'index']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")

def create_config():
    """Create config.json with default settings if it doesn't exist"""
    config_file = 'config.json'
    default_config = {
        "evaluate_name": True,
        "evaluate_license": True,
        "evaluate_exams": True,
        "evaluate_disclosures": True
    }
    
    if not os.path.exists(config_file):
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=4)
        print(f"Created config file with default settings")

if __name__ == "__main__":
    create_folders()
    create_config()

setup(
    name="milton-agents",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "pytest>=6.0.0",
        "pytest-mock>=3.6.0",
    ],
    python_requires=">=3.8",
)
