# cli.py
"""
🌟 Command-line interface for Cache Manager operations 🌟
"""

import argparse
from pathlib import Path
from .cache_operations import CacheManager
from .compliance_handler import ComplianceHandler
from .summary_generator import SummaryGenerator

def main():
    parser = argparse.ArgumentParser(description="🌟 Cache Manager CLI 🌟")
    parser.add_argument("--cache-folder", help="Override the default cache folder location")
    parser.add_argument("--clear-cache", nargs="?", const="ALL", help="Clear all cache (except ComplianceReportAgent) for an employee or all employees if 'ALL' or no value is specified")
    parser.add_argument("--clear-agent", nargs=2, metavar=("EMPLOYEE_NUMBER", "AGENT_NAME"), help="Clear cache for a specific agent")
    parser.add_argument("--list-cache", nargs="?", const="ALL", help="List all cached files (or specify an employee)")
    parser.add_argument("--cleanup-stale", action="store_true", help="Delete stale cache older than 90 days")
    parser.add_argument("--get-latest-compliance", help="Get the latest compliance report for an employee")
    parser.add_argument("--get-compliance-by-ref", nargs=2, metavar=("EMPLOYEE_NUMBER", "REFERENCE_ID"), help="Get compliance report by reference ID")
    parser.add_argument("--list-compliance-reports", nargs="?", const=None, help="List all compliance reports with latest revision (or specify an employee)")
    parser.add_argument("--generate-compliance-summary", help="Generate a compliance summary for a specific employee")
    parser.add_argument("--generate-all-summaries", action="store_true", help="Generate a compliance summary for all employees")
    parser.add_argument("--generate-compliance-taxonomy", action="store_true", help="Generate a taxonomy tree from the latest ComplianceReportAgent JSON files")
    parser.add_argument("--generate-risk-dashboard", action="store_true", help="Generate a compliance risk dashboard from the latest reports")
    parser.add_argument("--generate-data-quality", action="store_true", help="Generate a data quality report from the latest reports")
    parser.add_argument("--page", type=int, default=1, help="Page number for paginated results (default: 1)")
    parser.add_argument("--page-size", type=int, default=10, help="Number of items per page (default: 10)")

    args = parser.parse_args()
    cache_folder = Path(args.cache_folder) if args.cache_folder else None
    cache_manager = CacheManager(cache_folder=cache_folder) if cache_folder else CacheManager()
    compliance_handler = ComplianceHandler(cache_folder=cache_folder) if cache_folder else ComplianceHandler()
    summary_generator = SummaryGenerator(file_handler=cache_manager.file_handler, compliance_handler=compliance_handler)

    if args.clear_cache:
        if args.clear_cache == "ALL":
            print(cache_manager.clear_all_cache())
        else:
            print(cache_manager.clear_cache(args.clear_cache))
    elif args.clear_agent:
        employee, agent = args.clear_agent
        print(cache_manager.clear_agent_cache(employee, agent))
    elif args.list_cache is not None:
        print(cache_manager.list_cache(args.list_cache, args.page, args.page_size))
    elif args.cleanup_stale:
        print(cache_manager.cleanup_stale_cache())
    elif args.get_latest_compliance:
        print(compliance_handler.get_latest_compliance_report(args.get_latest_compliance))
    elif args.get_compliance_by_ref:
        employee, ref_id = args.get_compliance_by_ref
        print(compliance_handler.get_compliance_report_by_ref(employee, ref_id))
    elif args.list_compliance_reports is not None:
        print(compliance_handler.list_compliance_reports(args.list_compliance_reports, args.page, args.page_size))
    elif args.generate_compliance_summary:
        emp_path = cache_manager.cache_folder / args.generate_compliance_summary
        print(summary_generator.generate_compliance_summary(emp_path, args.generate_compliance_summary, args.page, args.page_size))
    elif args.generate_all_summaries:
        print(summary_generator.generate_all_compliance_summaries(cache_manager.cache_folder, args.page, args.page_size))
    elif args.generate_compliance_taxonomy:
        print(summary_generator.generate_taxonomy_from_latest_reports())
    elif args.generate_risk_dashboard:
        print(summary_generator.generate_risk_dashboard())
    elif args.generate_data_quality:
        print(summary_generator.generate_data_quality_report())
    else:
        parser.print_help()

if __name__ == "__main__":
    main()