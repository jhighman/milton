import os

def create_readme():
    readme_content = """# Project: CRD Evaluation Framework

## Overview

This project is a CRD (Central Registration Depository) evaluation framework that processes CSV files containing CRD numbers. It evaluates multiple criteria including name matching, license verification, exam requirements, and disclosures using API data from BrokerCheck, and it logs the results. The system includes a caching mechanism to reduce redundant API calls and uses a checkpoint system to ensure resilience against interruptions.

### Key Features
- **API Caching**: Cached responses to avoid repeated API calls.
- **Checkpoint System**: Safely handles interruptions by saving progress.
- **Modular Design**: Organized into separate modules for better maintainability and testing.
- **Configurable Evaluations**: Evaluations (name, license, exams, disclosures) can be turned on or off via a configuration file (`config.json`).

---

## Project Structure