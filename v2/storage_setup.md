# Storage Setup Guide

This document provides guidance on configuring storage for the application, including both local filesystem and AWS S3 storage options.

## Storage Configuration

The application supports two storage modes:
1. Local filesystem storage (default)
2. AWS S3 storage

### Configuration File

Storage settings are configured in `config.json`:

```json
{
  "storage": {
    "mode": "local",  // or "s3"
    "local": {
      "input_folder": "./drop",
      "output_folder": "./output",
      "archive_folder": "./output/archive",
      "cache_folder": "./cache"
    },
    "s3": {
      "aws_region": "us-east-1",
      "input_bucket": "cenxtgen-dev-brokersearch",
      "input_prefix": "fmrdb/input/",
      "output_bucket": "cenxtgen-dev-brokersearch-response",
      "output_prefix": "fmrdb/output/",
      "archive_bucket": "cenxtgen-dev-brokersearch",
      "archive_prefix": "fmrdb/input-archive/",
      "cache_bucket": "cenxtgen-dev-brokersearch-response",
      "cache_prefix": "fmrdb/cache/"
    }
  }
}
```

## AWS S3 Setup

### S3 Bucket Naming Convention

We recommend the following naming convention for S3 buckets:

```
{company}-{environment}-{purpose}
```

Example:
```
cenxtgen-dev-brokersearch
cenxtgen-dev-brokersearch-response
```

Components:
- `{company}`: Your company name (e.g., "cenxtgen")
- `{environment}`: Environment identifier (e.g., "dev", "prod")
- `{purpose}`: Purpose of the bucket (e.g., "brokersearch", "brokersearch-response")

### Bucket Structure

For each environment, create the following buckets:

1. Input Bucket:
   - Purpose: Receives incoming CSV files
   - Example: `cenxtgen-dev-brokersearch`
   - Prefix: `fmrdb/input/`

2. Output Bucket:
   - Purpose: Stores processed results
   - Example: `cenxtgen-dev-brokersearch-response`
   - Prefix: `fmrdb/output/`

3. Archive Location:
   - Purpose: Stores processed input files
   - Bucket: Same as input bucket
   - Prefix: `fmrdb/input-archive/`

4. Cache Location:
   - Purpose: Stores cached compliance data
   - Bucket: Same as output bucket
   - Prefix: `fmrdb/cache/`

### S3 Bucket Configuration

1. Versioning:
   - Enable versioning on all buckets for data protection
   - Helps recover from accidental deletions or overwrites

2. Lifecycle Rules:
   - Configure lifecycle rules to manage data retention
   - Example: Move objects to Glacier after 90 days
   - Example: Delete objects after 1 year

3. Encryption:
   - Enable server-side encryption (SSE) using AWS KMS
   - Use bucket policies to enforce encryption

4. Access Control:
   - Use IAM roles and policies for access control
   - Follow principle of least privilege
   - Enable bucket logging for audit trails

### AWS Credentials

1. Environment Variables:
   ```bash
   export AWS_ACCESS_KEY_ID="your_access_key"
   export AWS_SECRET_ACCESS_KEY="your_secret_key"
   export AWS_DEFAULT_REGION="us-east-1"
   ```

2. AWS CLI Configuration:
   ```bash
   aws configure
   ```

3. IAM User Requirements:
   - Minimum permissions for S3 operations
   - Access to specific buckets only
   - No root access

## Local Storage Setup

When using local storage mode:

1. Directory Structure:
   ```
   project_root/
   ├── drop/          # Input files
   ├── output/        # Processed results
   │   └── archive/   # Processed input files
   └── cache/         # Cached compliance data
   ```

2. Permissions:
   - Ensure application has read/write access
   - Use appropriate file system permissions
   - Consider using dedicated service account

## Migration Guide

### Local to S3 Migration

1. Create S3 buckets following naming convention
2. Update `config.json` with S3 settings
3. Use AWS CLI to migrate data:
   ```bash
   # Migrate input files
   aws s3 sync drop/ s3://cenxtgen-dev-brokersearch/fmrdb/input/
   
   # Migrate output files
   aws s3 sync output/ s3://cenxtgen-dev-brokersearch-response/fmrdb/output/
   
   # Migrate archive files
   aws s3 sync output/archive/ s3://cenxtgen-dev-brokersearch/fmrdb/input-archive/
   
   # Migrate cache files
   aws s3 sync cache/ s3://cenxtgen-dev-brokersearch-response/fmrdb/cache/
   ```

### S3 to Local Migration

1. Update `config.json` to use local storage
2. Use AWS CLI to download data:
   ```bash
   # Download input files
   aws s3 sync s3://cenxtgen-dev-brokersearch/fmrdb/input/ drop/
   
   # Download output files
   aws s3 sync s3://cenxtgen-dev-brokersearch-response/fmrdb/output/ output/
   
   # Download archive files
   aws s3 sync s3://cenxtgen-dev-brokersearch/fmrdb/input-archive/ output/archive/
   
   # Download cache files
   aws s3 sync s3://cenxtgen-dev-brokersearch-response/fmrdb/cache/ cache/
   ```

## Troubleshooting

### Common Issues

1. S3 Access Denied:
   - Check IAM permissions
   - Verify bucket policies
   - Confirm AWS credentials

2. Local Storage Issues:
   - Check directory permissions
   - Verify disk space
   - Confirm file system access

3. Configuration Problems:
   - Validate JSON syntax
   - Check for missing required fields
   - Verify storage mode setting

### Logging

Storage operations are logged with the following levels:
- INFO: Normal operations
- WARNING: Non-critical issues
- ERROR: Critical failures

Check application logs for detailed error messages and troubleshooting information. 