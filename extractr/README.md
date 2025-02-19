## Organization CRD Lookup

Organization data is stored in `input/organizationsCrd.jsonl` in JSON Lines format.
Each line contains a complete JSON record with:
- entityName: Organization name
- organizationCRD: CRD number
- normalizedName: Lowercase, simplified name for matching
- Additional metadata (address, tax ID, etc.) 