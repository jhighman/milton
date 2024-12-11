import os

# Define the cache folder path
cache_folder = './cache2'

def clear_sec_cache(cache_folder):
    """
    Loops through the cache directory and removes files that start with 'sec_'.

    Parameters:
        cache_folder (str): The path to the cache directory.
    """
    if not os.path.exists(cache_folder):
        print(f"Cache folder '{cache_folder}' does not exist.")
        return

    removed_files_count = 0

    # Walk through the directory
    for root, _, files in os.walk(cache_folder):
        for file in files:
            if file.startswith("sec_"):
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    print(f"Removed: {file_path}")
                    removed_files_count += 1
                except Exception as e:
                    print(f"Error removing {file_path}: {e}")

    print(f"\n--- Summary ---")
    print(f"Total 'sec_' files removed: {removed_files_count}")

if __name__ == "__main__":
    clear_sec_cache(cache_folder)
