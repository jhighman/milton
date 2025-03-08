# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          ____ _   _ ____  _____   ✨
#         / ___| | | |  _ \| ____|  🌟
#        | |   | |_| | |_) |  _|    🌟
#        | |___|  _  |  __/| |___   🌟
#         \____|_| |_|_|   |_____|  ✨
#
#          A Beautiful Cache Management Solution 🌟
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟

# 📜 README for Cache Manager
#
# This file contains the documentation for the Cache Manager package, formatted with
# Python comment conventions and adorned with emojis for a delightful experience. 🌈
# To convert this to README.md, copy everything below the script (excluding the Python
# code) into a new file named README.md, or run this script to generate it automatically. 🚀

# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          🌟 CACHE MANAGER 🌟
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# 🖼️ [Banner Placeholder: https://via.placeholder.com/800x200.png?text=Cache+Manager]
#
# +-----------------------------------------------------------+
# |                   📌 OVERVIEW                             |
# +-----------------------------------------------------------+
#
# Welcome to *Cache Manager* 🌟, a robust Python package crafted to handle cached data
# for regulatory and compliance reporting. Designed with modularity and maintainability
# at its core 💡, this package organizes cache by employee identifiers (e.g., `EMP001`,
# `LD-107-Dev-3A`) and supports a variety of agents, with special handling for compliance
# reports. Whether you're clearing stale cache 🧹, retrieving the latest compliance reports 📋,
# or generating detailed summaries 📊, Cache Manager offers an elegant solution.
#
# 🌿 Purpose
# ~~~~~~~
# Cache Manager streamlines regulatory data management with a structured cache system
# and a suite of intuitive tools. It’s perfect for compliance teams 👥, developers 💻,
# and system administrators 🖥️ seeking reliable, version-aware cache operations.
#
# ✨ Key Features
# ~~~~~~~~~~~~
# - 🌐 Modular Design: Organized into focused modules (`cache_operations`, `compliance_handler`, etc.)
#   for clarity and extensibility.
# - 🧹 Comprehensive Cache Management: Effortlessly clear, list, and clean up cache.
# - 📋 Compliance Report Handling: Retrieve and summarize reports with versioning and pagination.
# - 📄 JSON Outputs: All results delivered as JSON strings for seamless integration.
# - 🖱️ Command-Line Interface (CLI): A user-friendly CLI for rapid operations.
# - ⏰ Automatic Cleanup: Purges stale cache older than 90 days.
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          📂 CACHE FOLDER STRUCTURE
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# Cached data resides under a `cache/` directory, structured as follows:
#
# +-------------------+
# | 📁 cache/         |
# | ├── LD-107-Dev-3A/|
# | │   ├── SEC_IAPD_Agent/
# | │   │   ├── search_individual/
# | │   │   │   ├── SEC_IAPD_Agent_LD-107-Dev-3A_search_individual_20240307.json
# | │   │   │   ├── manifest.txt
# | │   ├── FINRA_BrokerCheck_Agent/
# | │   ├── ComplianceReportAgent_EN-53_v1_20250308.json
# | │   ├── request_log.txt
# | ├── EMP002/
# | │   ├── SEC_IAPD_Agent/
# | │   └── ComplianceReportAgent_EN-54_v2_20250309.json
# +-------------------+
#
# - 📂 Agent Folders: House agent-specific data (e.g., `SEC_IAPD_Agent`).
# - 📜 Compliance Reports: Stored directly under employee folders with versioning (e.g., `v1`).
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          🚀 GETTING STARTED
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# 🛠️ Prerequisites
# ~~~~~~~~~~~~~
# - Python 3.7+ 🐍
# - No external dependencies beyond the standard library 📦
#
# 📥 Installation
# ~~~~~~~~~~~~
# 1. Clone or download the repository:
#    git clone https://github.com/yourusername/cache_manager.git
#    cd cache_manager
# 2. (Optional) Install as a package:
#    pip install .
#
# 🎯 Quick Usage
# ~~~~~~~~~~~
# Explore features via the CLI:
# python -m cache_manager.cli --list-cache LD-107-Dev-3A
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          🛠️ MODULES AND USAGE
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# Cache Manager is divided into modular components, each with a distinct purpose.
# Below are the key modules and their usage examples.
#
# 1. cache_operations.CacheManager
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 🎯 Purpose: Manages general cache operations (clearing, listing, cleanup).
#
# 📌 Example: Clear Cache
# ^^^^^^^^^^^^^^^^^^^
# from cache_manager.cache_operations import CacheManager
# manager = CacheManager()
# print(manager.clear_cache("LD-107-Dev-3A"))
#
# 📜 Output:
# {
#   "employee_number": "LD-107-Dev-3A",
#   "cleared_agents": ["SEC_IAPD_Agent"],
#   "status": "success",
#   "message": "Cleared cache for 1 agents"
# }
#
# 📌 Example: List All Cache
# ^^^^^^^^^^^^^^^^^^^^^^^
# print(manager.list_cache())
#
# 📜 Output:
# {
#   "status": "success",
#   "message": "Listed all employees with cache",
#   "cache": {
#     "employees": ["LD-107-Dev-3A", "EMP002"]
#   }
# }
#
# 2. compliance_handler.ComplianceHandler
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 🎯 Purpose: Oversees compliance report retrieval and listing.
#
# 📌 Example: Get Latest Compliance Report
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# from cache_manager.compliance_handler import ComplianceHandler
# handler = ComplianceHandler()
# print(handler.get_latest_compliance_report("LD-107-Dev-3A"))
#
# 📜 Output:
# {
#   "employee_number": "LD-107-Dev-3A",
#   "status": "success",
#   "message": "Retrieved latest compliance report: ComplianceReportAgent_EN-53_v1_20250308.json",
#   "report": {
#     "reference_id": "EN-53",
#     "final_evaluation": {"overall_compliance": "Compliant", "alerts": []}
#   }
# }
#
# 📌 Example: List Compliance Reports
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# print(handler.list_compliance_reports("LD-107-Dev-3A", page=1, page_size=5))
#
# 📜 Output:
# {
#   "employee_number": "LD-107-Dev-3A",
#   "status": "success",
#   "message": "Listed 1 compliance reports for LD-107-Dev-3A",
#   "reports": [
#     {
#       "reference_id": "EN-53",
#       "file_name": "ComplianceReportAgent_EN-53_v1_20250308.json",
#       "last_modified": "2025-03-08 14:30:22"
#     }
#   ],
#   "pagination": {
#     "total_items": 1,
#     "total_pages": 1,
#     "current_page": 1,
#     "page_size": 5
#   }
# }
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          🎮 COMMAND-LINE INTERFACE (CLI)
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# The package includes a powerful CLI for swift operations. Run
# python -m cache_manager.cli --help for a full list of options. 🎯
#
# 🚀 Usage Examples
# ~~~~~~~~~~~~~~
# 1. Clear Cache for an Employee:
#    python -m cache_manager.cli --clear-cache LD-107-Dev-3A
#
# 2. List Compliance Reports for All Employees:
#    python -m cache_manager.cli --list-compliance-reports --page 1 --page-size 10
#
# 3. Get Latest Compliance Report:
#    python -m cache_manager.cli --get-latest-compliance LD-107-Dev-3A
#
# 4. Clean Up Stale Cache:
#    python -m cache_manager.cli --cleanup-stale
#
# 5. Custom Cache Folder:
#    python -m cache_manager.cli --cache-folder /custom/path --list-cache ALL
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          🧰 FULL FEATURE LIST
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# 🔧 Cache Operations (cache_operations.CacheManager)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# - clear_cache(employee_number): Clears all agent caches except compliance reports.
# - clear_agent_cache(employee_number, agent_name): Clears a specific agent’s cache.
# - list_cache(employee_number=None): Lists cache for all or one employee.
# - cleanup_stale_cache(): Removes files older than 90 days (excluding compliance reports).
#
# 📋 Compliance Handling (compliance_handler.ComplianceHandler)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# - get_latest_compliance_report(employee_number): Retrieves the latest report with versioning.
# - get_compliance_report_by_ref(employee_number, reference_id): Retrieves a report by reference ID.
# - list_compliance_reports(employee_number=None, page=1, page_size=10): Lists reports with pagination.
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          🔍 TROUBLESHOOTING
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# ⚠️ Common Issues and Fixes
# ~~~~~~~~~~~~~~~~~~~~~~~
# 1. No Cache Found:
#    - Symptom: Methods return "status": "warning" with "No cache found" message.
#    - Fix: Verify cache_folder path exists (ls -ld <cache_folder> or dir <cache_folder>).
#    - Check: Ensure read permissions with os.access(path, os.R_OK) in Python.
#
# 2. Empty Report Listings:
#    - Symptom: list_compliance_reports returns no results.
#    - Fix: Confirm files match ComplianceReportAgent_*_v*.json pattern.
#    - Check: Inspect folder contents with ls -l <cache_folder/employee>.
#
# 3. Permission Errors:
#    - Symptom: Logs show "Failed to delete" or "not readable".
#    - Fix: Adjust folder permissions (e.g., chmod -R u+rw <cache_folder> on Unix).
#
# 4. Stale Cache Not Removed:
#    - Symptom: cleanup_stale_cache deletes nothing.
#    - Fix: Verify CACHE_TTL_DAYS (default: 90) and file timestamps.
#
# 🛠️ Debugging Tips
# ~~~~~~~~~~~~~~
# - Enable detailed logging by editing config.py:
#    LOG_LEVEL = "INFO"  # or "DEBUG" for more detail
# - Check logs for warnings/errors: tail -f cache_manager.log (if redirected).
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          🌟 CONTRIBUTING
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# We welcome contributions! Here’s how to get involved: 🤝
# 1. Fork the repository. 🍴
# 2. Create a feature branch (git checkout -b feature/awesome-addition). 🌿
# 3. Commit changes (git commit -m "Add awesome feature"). ✅
# 4. Push to your fork (git push origin feature/awesome-addition). 🚀
# 5. Open a Pull Request. 📬
#
# Please include tests and update documentation for new features. 📝
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          📜 LICENSE
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#
# This project is licensed under the MIT License. See [LICENSE](LICENSE) for details. ⚖️
#
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
#          🙏 ACKNOWLEDGMENTS
# 🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟🌟
