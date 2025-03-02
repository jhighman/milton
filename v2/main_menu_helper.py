import logging
from typing import Dict, Set

logger = logging.getLogger('main_menu_helper')

def display_menu(skip_disciplinary: bool, skip_arbitration: bool, skip_regulatory: bool, wait_time: float) -> str:
    print("\nCompliance CSV Processor Menu:")
    print("1. Run batch processing")
    print(f"2. Toggle disciplinary review (currently: {'skipped' if skip_disciplinary else 'enabled'})")
    print(f"3. Toggle arbitration review (currently: {'skipped' if skip_arbitration else 'enabled'})")
    print(f"4. Toggle regulatory review (currently: {'skipped' if skip_regulatory else 'enabled'})")
    print("5. Save settings")
    print("6. Manage logging groups")
    print("7. Flush logs")
    print("8. Set trace mode (all groups on, DEBUG level)")
    print("9. Set production mode (minimal logging)")
    print(f"10. Set wait time between records (currently: {wait_time} seconds)")
    print("11. Exit")
    return input("Enter your choice (1-11): ").strip()

def handle_menu_choice(choice: str, skip_disciplinary: bool, skip_arbitration: bool, skip_regulatory: bool, 
                       enabled_groups: Set[str], group_levels: Dict[str, str], wait_time: float, 
                       config: Dict[str, object], loggers: Dict[str, logging.Logger], 
                       LOG_LEVELS: Dict[str, tuple], save_config_func, flush_logs_func) -> tuple:
    if choice == "2":
        skip_disciplinary = not skip_disciplinary
        logger.info(f"Disciplinary review {'skipped' if skip_disciplinary else 'enabled'}")
        print(f"Disciplinary review is now {'skipped' if skip_disciplinary else 'enabled'}")
    elif choice == "3":
        skip_arbitration = not skip_arbitration
        logger.info(f"Arbitration review {'skipped' if skip_arbitration else 'enabled'}")
        print(f"Arbitration review is now {'skipped' if skip_arbitration else 'enabled'}")
    elif choice == "4":
        skip_regulatory = not skip_regulatory
        logger.info(f"Regulatory review {'skipped' if skip_regulatory else 'enabled'}")
        print(f"Regulatory review is now {'skipped' if skip_regulatory else 'enabled'}")
    elif choice == "5":
        config.update({
            "skip_disciplinary": skip_disciplinary,
            "skip_arbitration": skip_arbitration,
            "skip_regulatory": skip_regulatory,
            "enabled_logging_groups": list(enabled_groups),
            "logging_levels": dict(group_levels)
        })
        save_config_func(config)
        print(f"Settings saved to {config['config_file']}")
    elif choice == "6":
        manage_logging_groups(enabled_groups, group_levels, LOG_LEVELS)
    elif choice == "7":
        flush_logs_func()
        print("Logs flushed")
    elif choice == "8":
        enabled_groups.clear()
        enabled_groups.update({"services", "agents", "evaluation", "core"})
        group_levels.update({group: "DEBUG" for group in enabled_groups})
        print("Trace mode enabled: all groups ON, level DEBUG")
    elif choice == "9":
        enabled_groups.clear()
        enabled_groups.add("core")
        group_levels.update({"core": "INFO", "services": "WARNING", "agents": "WARNING", "evaluation": "WARNING"})
        print("Production mode enabled: minimal logging (core INFO, others OFF)")
    elif choice == "10":
        try:
            new_wait_time = float(input(f"Enter wait time in seconds (current: {wait_time}, default: {config['default_wait_time']}): ").strip())
            if new_wait_time >= 0:
                wait_time = new_wait_time
                logger.info(f"Wait time set to {wait_time} seconds")
                print(f"Wait time set to {wait_time} seconds")
            else:
                print("Wait time must be non-negative")
        except ValueError:
            print("Invalid input. Please enter a number")
    elif choice == "11":
        logger.info("User chose to exit")
        print("Exiting...")
    else:
        logger.warning(f"Invalid menu choice: {choice}")
        print("Invalid choice. Please enter 1-11.")
    return skip_disciplinary, skip_arbitration, skip_regulatory, enabled_groups, group_levels, wait_time

def manage_logging_groups(enabled_groups: Set[str], group_levels: Dict[str, str], LOG_LEVELS: Dict[str, tuple]):
    print("\nLogging Groups Management:")
    print("Available groups: services, agents, evaluation, core")
    for group in ["services", "agents", "evaluation", "core"]:
        status = "enabled" if group in enabled_groups else "disabled"
        level = group_levels.get(group, "INFO")
        print(f"{group} - {status}, Level: {level}")
    print("\nOptions:")
    print("1. Toggle group on/off")
    print("2. Set group level")
    print("3. Back")
    sub_choice = input("Enter your choice (1-3): ").strip()

    if sub_choice == "1":
        group = input("Enter group name (services/agents/evaluation/core): ").strip().lower()
        if group in ["services", "agents", "evaluation", "core"]:
            if group in enabled_groups:
                enabled_groups.remove(group)
                logger.info(f"Disabled logging group: {group}")
                print(f"{group} logging disabled")
            else:
                enabled_groups.add(group)
                logger.info(f"Enabled logging group: {group}")
                print(f"{group} logging enabled")
        else:
            print("Invalid group name")
    elif sub_choice == "2":
        group = input("Enter group name (services/agents/evaluation/core): ").strip().lower()
        if group in ["services", "agents", "evaluation", "core"]:
            print("Levels: 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL")
            level_choice = input("Enter level (1-5): ").strip()
            if level_choice in LOG_LEVELS:
                group_levels[group] = LOG_LEVELS[level_choice][0]
                logger.info(f"Set {group} logging level to {LOG_LEVELS[level_choice][0]}")
                print(f"{group} level set to {LOG_LEVELS[level_choice][0]}")
            else:
                print("Invalid level choice")
        else:
            print("Invalid group name")
    elif sub_choice != "3":
        print("Invalid choice")