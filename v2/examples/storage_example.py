"""
Example script demonstrating usage of the storage system.
"""

import os
import json
from storage_manager import StorageManager

def main():
    """Main function demonstrating storage operations."""
    # Initialize storage manager with default configuration
    storage = StorageManager()
    
    # Create some test content
    test_content = "Hello, World!"
    
    # Write content to a file
    print("Writing test content...")
    success = storage.write_file("test.txt", test_content)
    if success:
        print("Successfully wrote content to test.txt")
    
    # Read the file back
    print("\nReading test content...")
    content = storage.read_file("test.txt")
    print(f"Read content: {content.decode()}")
    
    # List files in the directory
    print("\nListing files...")
    files = storage.list_files(".")
    print("Files found:", files)
    
    # Get file information
    print("\nGetting file information...")
    size = storage.get_file_size("test.txt")
    mtime = storage.get_file_modified_time("test.txt")
    print(f"File size: {size} bytes")
    print(f"Last modified: {mtime}")
    
    # Move the file
    print("\nMoving file...")
    success = storage.move_file("test.txt", "moved.txt")
    if success:
        print("Successfully moved file")
    
    # Verify the move
    print("\nVerifying move...")
    exists_old = storage.file_exists("test.txt")
    exists_new = storage.file_exists("moved.txt")
    print(f"Original file exists: {exists_old}")
    print(f"New file exists: {exists_new}")
    
    # Create a directory
    print("\nCreating directory...")
    success = storage.create_directory("test_dir")
    if success:
        print("Successfully created directory")
    
    # Write a file in the new directory
    print("\nWriting file in new directory...")
    success = storage.write_file("test_dir/nested.txt", "Nested content")
    if success:
        print("Successfully wrote nested file")
    
    # List files in the new directory
    print("\nListing files in new directory...")
    files = storage.list_files("test_dir")
    print("Files found:", files)
    
    # Clean up
    print("\nCleaning up...")
    storage.delete_file("moved.txt")
    storage.delete_file("test_dir/nested.txt")
    print("Cleanup complete")

def s3_example():
    """Example using S3 storage."""
    # Create S3 configuration
    config = {
        "storage": {
            "mode": "s3",
            "s3": {
                "aws_region": "us-east-1",
                "input_bucket": "my-input-bucket",
                "input_prefix": "input/",
                "output_bucket": "my-output-bucket",
                "output_prefix": "output/",
                "archive_bucket": "my-archive-bucket",
                "archive_prefix": "archive/",
                "cache_bucket": "my-cache-bucket",
                "cache_prefix": "cache/"
            }
        }
    }
    
    # Save configuration
    with open("s3_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    # Initialize storage manager with S3 configuration
    storage = StorageManager("s3_config.json")
    
    # Perform the same operations as in the main example
    test_content = "Hello from S3!"
    
    print("Writing test content to S3...")
    success = storage.write_file("test.txt", test_content)
    if success:
        print("Successfully wrote content to S3")
    
    print("\nReading test content from S3...")
    content = storage.read_file("test.txt")
    print(f"Read content: {content.decode()}")
    
    print("\nListing files in S3...")
    files = storage.list_files(".")
    print("Files found:", files)
    
    print("\nCleaning up...")
    storage.delete_file("test.txt")
    print("Cleanup complete")
    
    # Clean up configuration file
    os.remove("s3_config.json")

if __name__ == "__main__":
    print("=== Local Storage Example ===")
    main()
    
    print("\n=== S3 Storage Example ===")
    s3_example() 