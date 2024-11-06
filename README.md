## README

### Overview
This repository contains two Python scripts developed for the CSA Challenge at Atlan. The scripts are designed to:

- **Ingest S3 Objects into Atlan**: `build_connection_db_schema_columns.py` ingests CSV files from a specified S3 bucket into Atlan as assets, creating a connection, database, schema, tables, and columns based on the CSV files' structure.
- **Establish Lineage Between Assets**: `build_table_column_lineage.py` establishes upstream and downstream lineage between Postgres, S3, and Snowflake assets in Atlan using the Atlan SDK.

### Table of Contents
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Scripts Overview](#scripts-overview)
  - [build_connection_db_schema_columns.py](#build_connection_db_schema_columnspy)
  - [build_table_column_lineage.py](#build_table_column_lineagepy)
- [Usage](#usage)
  - [Running build_connection_db_schema_columns.py](#running-build_connection_db_schema_columnspy)
  - [Running build_table_column_lineage.py](#running-build_table_column_lineagepy)
- [Logging](#logging)
- [Notes](#notes)
- [References](#references)

### Prerequisites
- Python 3.7+ installed on your system.
- Atlan SDK for Python (`pyatlan`) installed.
- Boto3 installed for AWS S3 interactions.
- Pandas installed for CSV processing.

**Atlan Account Credentials:**
- `BASE_URL`: Your Atlan instance URL.
- `API_TOKEN`: Your Atlan API token.

**AWS S3 Access:**
- Public access to the specified S3 bucket (`atlan-tech-challenge`).

### Configuration
Create a `config.py` file in the same directory as the scripts with the following content:

```python
# config.py

# Atlan configuration
BASE_URL = "https://your-atlan-instance.atlan.com"
API_TOKEN = "your-api-token"

# Logging configuration
LOG_FILE = "app.log"
LOG_LEVEL = "INFO"

# S3 configuration
S3_BUCKET_NAME = "atlan-tech-challenge"
S3_OBJECT_PREFIX = ""  # Empty since the files are at the root of the bucket
S3_OBJECT_PATTERN = r"^[^/]+\.csv$"  # Matches CSV files at the root level only

# Connection and asset names
AWS_CONNECTION_NAME = "aws-s3-connection-xx"  # Replace 'xx' with your initials or unique identifier
POSTGRES_CONNECTION_NAME = 'postgres-xx' # Replace 'xx' with your initials or unique identifier
SNOWFLAKE_CONNECTION_NAME = 'snowflake-xx' # Replace 'xx' with your initials or unique identifier
DATABASE_NAME = "your_database_name"
SCHEMA_NAME = "your_schema_name"

# Admin groups and users for Atlan
ADMIN_GROUPS = ['admin_group']
ADMIN_USERS = ['your_id']
```

**Note**: Replace placeholders with your actual configuration values.

### Scripts Overview

#### build_connection_db_schema_columns.py
This script performs the following tasks:

1. **Create or Retrieve S3 Connection**: Ensures that an S3 connection exists in Atlan.
2. **Create or Retrieve Database and Schema**: Sets up a database and schema under the S3 connection.
3. **Process CSV Files from S3**:
   - Fetches specified CSV files from the S3 bucket using boto3.
   - Loads the files into a pandas DataFrame to extract column names and data types.
   - Creates Table and Column assets in Atlan based on the CSV files' structure using pyatlan.
   - Updates Column assets with accurate data types mapped from the DataFrame.

#### build_table_column_lineage.py
This script establishes lineage between assets:

1. **Retrieve Connections**: Fetches the qualified names of Postgres, S3, and Snowflake connections.
2. **Retrieve Tables**: Searches for tables under each connection.
3. **Establish Table Lineage**:
   - Matches tables between Postgres → S3 and S3 → Snowflake based on table names.
   - Creates Process entities in Atlan to represent table-level lineage.
4. **Establish Column Lineage**:
   - Retrieves columns for each matched table.
   - Creates ColumnProcess entities in Atlan to represent column-level lineage.

### Usage

#### Running build_connection_db_schema_columns.py
1. **Ensure Configuration is Set**: Update `config.py` with your settings.

2. **Run the Script**:

   ```bash
   python build_connection_db_schema_columns.py
   ```

3. **Script Output**:
   - The script will log its progress to the console and to `app.log`.
   - It will create the necessary assets in Atlan based on the CSV files from S3.

#### Running table_column_lineage_qbr_v2.py
1. **Ensure Configuration is Set**: Update `config.py` with your settings, especially the connection names.

2. **Run the Script**:

   ```bash
   python build_table_column_lineage.py
   ```

3. **Script Output**:
   - The script will display the tables found under each connection.
   - It will prompt you to confirm before proceeding with lineage establishment.
   - Lineage between tables and columns will be created in Atlan.

### Logging
Both scripts utilize Python's logging module to record their operations. Logs are written to `app.log` as specified in `config.py`.

- **Log Levels**: Configurable via `LOG_LEVEL` in `config.py` (e.g., INFO, DEBUG, ERROR).
- **Log File**: The default log file is `app.log`.

### Notes
- **S3 Bucket Access**: The S3 bucket `atlan-tech-challenge` is public. Ensure that the CSV file paths specified in `S3_OBJECT_NAMES` are correct.
- **Unique Asset Names**: When creating assets in Atlan, ensure that names are unique (e.g., by appending your initials) to avoid conflicts.
- **Error Handling**: The scripts include exception handling and will log errors encountered during execution.
- **Dependencies**: Ensure all required Python packages are installed:

  ```bash
  pip install pyatlan boto3 pandas
  ```

### References
- [Atlan Documentation](https://ask.atlan.com/)
- [Atlan Developer Portal](https://developer.atlan.com/)
- [Atlan SDK for Python](https://developer.atlan.com/sdks/python-details/)
- [AWS Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
