import logging
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from pyatlan.client.atlan import AtlanClient
from pyatlan.cache.role_cache import RoleCache
from pyatlan.model.assets import (
    Asset,
    Column,
    Connection,
    Database,
    Schema,
    Table,
)
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
from io import StringIO
import config

# Configure logging
logging.basicConfig(
    filename=config.LOG_FILE,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=getattr(logging, config.LOG_LEVEL.upper()),
)

# Initialize the Atlan client
client = AtlanClient(
    base_url=config.BASE_URL,
    api_key=config.API_TOKEN,
)

# Initialize the S3 client for anonymous access
s3_client = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED)
)

def get_existing_connection(name):
    """Retrieve an existing Connection by name using Fluent Search."""
    try:
        search = (
            FluentSearch()
            .where(CompoundQuery.asset_type(Connection))
            .where(Asset.STATUS.eq("ACTIVE"))
            .where(Connection.NAME.eq(name, case_insensitive=True))
            .page_size(1)
        ).to_request()
        
        search_results = client.asset.search(search)
        
        for asset in search_results.current_page():
            if isinstance(asset, Connection) and asset.name.lower() == name.lower():
                return asset
        return None
    except Exception as e:
        logging.error(f"Error searching for Connection '{name}': {e}")
        print(f"Error searching for Connection '{name}': {e}")
        raise

def create_s3_connection():
    """Create or retrieve an existing S3 connection in Atlan."""
    try:
        logging.info(f"Ensuring S3 connection '{config.AWS_CONNECTION_NAME}' exists.")
        # Attempt to retrieve existing connection
        existing_connection = get_existing_connection(config.AWS_CONNECTION_NAME)
        if existing_connection:
            logging.info(f"Found existing S3 Connection: {existing_connection.qualified_name}")
            return existing_connection.qualified_name
        else:
            # Create new connection
            print(f"Creating S3 connection '{config.AWS_CONNECTION_NAME}'...")
            admin_role_guid = RoleCache.get_id_for_name("$admin")
            connection = Connection.creator(
                name=config.AWS_CONNECTION_NAME,
                connector_type=AtlanConnectorType.S3,
                admin_roles=[admin_role_guid],
                admin_groups=config.ADMIN_GROUPS,  # From config.py
                admin_users=config.ADMIN_USERS,    # From config.py
            )
            response = client.asset.save(connection)
            if response and response.assets_created:
                connection_asset = response.assets_created(asset_type=Connection)[0]
                connection_qualified_name = connection_asset.qualified_name
                print(f"Created S3 Connection with Qualified Name: {connection_qualified_name}")
                logging.info(f"Created S3 Connection with Qualified Name: {connection_qualified_name}")
                return connection_qualified_name
            else:
                logging.error(f"Failed to create S3 Connection '{config.AWS_CONNECTION_NAME}'.")
                print(f"Failed to create S3 Connection '{config.AWS_CONNECTION_NAME}'.")
                return None
    except Exception as e:
        logging.error(f"Error ensuring S3 Connection: {e}")
        print(f"Error ensuring S3 Connection: {e}")
        raise

def get_existing_database(name, connection_qualified_name):
    """Retrieve an existing Database by name under a specific Connection."""
    try:
        search = (
            FluentSearch()
            .where(CompoundQuery.asset_type(Database))
            .where(Asset.STATUS.eq("ACTIVE"))
            .where(Database.NAME.eq(name, case_insensitive=True))
            .where(Database.CONNECTION_QUALIFIED_NAME.eq(connection_qualified_name))
            .page_size(1)
        ).to_request()
        
        search_results = client.asset.search(search)
        
        for asset in search_results.current_page():
            if isinstance(asset, Database) and asset.name.lower() == name.lower():
                return asset
        return None
    except Exception as e:
        logging.error(f"Error searching for Database '{name}': {e}")
        print(f"Error searching for Database '{name}': {e}")
        raise

def create_database(connection_qualified_name):
    """Create or retrieve an existing Database asset in Atlan."""
    try:
        database_name = config.DATABASE_NAME  # From config.py
        logging.info(f"Ensuring Database '{database_name}' exists under Connection '{connection_qualified_name}'.")
        
        existing_database = get_existing_database(database_name, connection_qualified_name)
        if existing_database:
            logging.info(f"Found existing Database: {existing_database.qualified_name}")
            return existing_database
        else:
            # Create new Database
            print(f"Creating Database '{database_name}' with connection qualified name '{connection_qualified_name}'...")
            database = Database.creator(
                name=database_name,
                connection_qualified_name=connection_qualified_name,
            )
            response = client.asset.save(database)
            if response and response.assets_created:
                database_asset = response.assets_created(asset_type=Database)[0]
                print(f"Created Database '{database_name}' with qualified name '{database_asset.qualified_name}'.")
                logging.info(f"Created Database '{database_name}' with qualified name '{database_asset.qualified_name}'.")
                return database_asset
            else:
                logging.error(f"Failed to create Database '{database_name}'.")
                print(f"Failed to create Database '{database_name}'.")
                return None
    except Exception as e:
        logging.error(f"Error ensuring Database: {e}")
        print(f"Error ensuring Database: {e}")
        raise

def get_existing_schema(name, database_qualified_name):
    """Retrieve an existing Schema by name under a specific Database."""
    try:
        search = (
            FluentSearch()
            .where(CompoundQuery.asset_type(Schema))
            .where(Asset.STATUS.eq("ACTIVE"))
            .where(Schema.NAME.eq(name, case_insensitive=True))
            .where(Schema.DATABASE_QUALIFIED_NAME.eq(database_qualified_name))
            .page_size(1)
        ).to_request()
        
        search_results = client.asset.search(search)
        
        for asset in search_results.current_page():
            if isinstance(asset, Schema) and asset.name.lower() == name.lower():
                return asset
        return None
    except Exception as e:
        logging.error(f"Error searching for Schema '{name}': {e}")
        print(f"Error searching for Schema '{name}': {e}")
        raise

def create_schema(database_qualified_name):
    """Create or retrieve an existing Schema asset in Atlan."""
    try:
        schema_name = config.SCHEMA_NAME  # From config.py
        logging.info(f"Ensuring Schema '{schema_name}' exists under Database '{database_qualified_name}'.")
        
        existing_schema = get_existing_schema(schema_name, database_qualified_name)
        if existing_schema:
            logging.info(f"Found existing Schema: {existing_schema.qualified_name}")
            return existing_schema
        else:
            # Create new Schema
            print(f"Creating Schema '{schema_name}' under Database '{database_qualified_name}'...")
            schema = Schema.creator(
                name=schema_name,
                database_qualified_name=database_qualified_name,
            )
            response = client.asset.save(schema)
            if response and response.assets_created:
                schema_asset = response.assets_created(asset_type=Schema)[0]
                print(f"Created Schema '{schema_name}' with qualified name '{schema_asset.qualified_name}'.")
                logging.info(f"Created Schema '{schema_name}' with qualified name '{schema_asset.qualified_name}'.")
                return schema_asset
            else:
                logging.error(f"Failed to create Schema '{schema_name}'.")
                print(f"Failed to create Schema '{schema_name}'.")
                return None
    except Exception as e:
        logging.error(f"Error ensuring Schema: {e}")
        print(f"Error ensuring Schema: {e}")
        raise

def get_existing_table(name, schema_qualified_name):
    """Retrieve an existing Table by name under a specific Schema."""
    try:
        search = (
            FluentSearch()
            .where(CompoundQuery.asset_type(Table))
            .where(Asset.STATUS.eq("ACTIVE"))
            .where(Table.NAME.eq(name, case_insensitive=True))
            .where(Table.SCHEMA_QUALIFIED_NAME.eq(schema_qualified_name))
            .page_size(1)
        ).to_request()
        
        search_results = client.asset.search(search)
        
        for asset in search_results.current_page():
            if isinstance(asset, Table) and asset.name.lower() == name.lower():
                return asset
        return None
    except Exception as e:
        logging.error(f"Error searching for Table '{name}': {e}")
        print(f"Error searching for Table '{name}': {e}")
        raise

def create_table(table_name, schema_qualified_name):
    """Create or retrieve an existing Table asset in Atlan."""
    try:
        logging.info(f"Ensuring Table '{table_name}' exists under Schema '{schema_qualified_name}'.")
        
        existing_table = get_existing_table(table_name, schema_qualified_name)
        if existing_table:
            logging.info(f"Found existing Table: {existing_table.qualified_name}")
            return existing_table
        else:
            # Create new Table
            print(f"Creating Table '{table_name}' under Schema '{schema_qualified_name}'...")
            table = Table.creator(
                name=table_name,
                schema_qualified_name=schema_qualified_name,
            )
            response = client.asset.save(table)
            if response and response.assets_created:
                table_asset = response.assets_created(asset_type=Table)[0]
                print(f"Created Table '{table_name}' with qualified name '{table_asset.qualified_name}'.")
                logging.info(f"Created Table '{table_name}' with qualified name '{table_asset.qualified_name}'.")
                return table_asset
            else:
                logging.error(f"Failed to create Table '{table_name}'.")
                print(f"Failed to create Table '{table_name}'.")
                return None
    except Exception as e:
        logging.error(f"Error ensuring Table: {e}")
        print(f"Error ensuring Table: {e}")
        raise

def extract_and_create_columns(csv_file_key, schema_qualified_name):
    """Extract schema from S3 CSV file and create Table and Column assets in Atlan."""
    try:
        print(f"\nProcessing CSV file: {csv_file_key}")
        logging.info(f"Starting processing of CSV file: {csv_file_key}")
        bucket_name = config.S3_BUCKET_NAME

        # Fetch the CSV file from S3
        print(f"Fetching CSV file '{csv_file_key}' from bucket '{bucket_name}'...")
        logging.info(f"Fetching CSV file '{csv_file_key}' from bucket '{bucket_name}'.")
        obj = s3_client.get_object(Bucket=bucket_name, Key=csv_file_key)
        csv_content = obj['Body'].read().decode('utf-8')

        # Use pandas to read the CSV file
        df = pd.read_csv(StringIO(csv_content))  # Read the entire file

        # Get column names and filter out unnamed columns
        column_names = [col for col in df.columns if not col.startswith('Unnamed')]
        print(f"Extracted column names: {column_names}")
        logging.info(f"Extracted column names: {column_names}")

        if not column_names:
            print(f"No valid columns found in '{csv_file_key}'. Skipping.")
            logging.warning(f"No valid columns found in '{csv_file_key}'. Skipping.")
            return

        # Get data types
        data_types = df.dtypes

        # Map pandas data types to Atlan data types
        pandas_to_atlan_types = {
            'int64': 'bigint',
            'int32': 'int',
            'float64': 'double',
            'float32': 'float',
            'object': 'string',
            'bool': 'boolean',
            'datetime64[ns]': 'timestamp',
            'timedelta[ns]': 'interval',
            # Add more mappings as needed
        }

        column_type_mapping = {}
        for col_name in column_names:
            pandas_type = str(data_types[col_name])
            atlan_type = pandas_to_atlan_types.get(pandas_type, 'string')  # Default to 'string' if not found
            column_type_mapping[col_name] = atlan_type

        # Create or retrieve Table asset
        table_name = csv_file_key.replace('/', '_').replace('.csv', '')  # Adjust table name as needed
        table_asset = create_table(table_name, schema_qualified_name)
        if not table_asset:
            print(f"Failed to create or retrieve Table '{table_name}'. Skipping columns creation.")
            logging.error(f"Failed to create or retrieve Table '{table_name}'. Skipping columns creation.")
            return

        # Step 1: Create Column assets without 'data_type' and save them
        column_assets = []
        for idx, col_name in enumerate(column_names, start=1):
            logging.info(f"Creating Column '{col_name}' (order {idx}) under Table '{table_name}'.")
            print(f"Creating Column '{col_name}' (order {idx}) under Table '{table_name}'...")

            column = Column.creator(
                name=col_name,
                parent_qualified_name=table_asset.qualified_name,
                parent_type=Table,
                order=idx,
            )
            column_assets.append(column)

        # Save all new columns at once
        if column_assets:
            response = client.asset.save(column_assets)
            if response and response.assets_created:
                print(f"Created {len(response.assets_created(asset_type=Column))} columns for Table '{table_name}'.")
                logging.info(f"Created {len(response.assets_created(asset_type=Column))} columns for Table '{table_name}'.")
            else:
                print(f"Failed to create columns for Table '{table_name}'.")
                logging.error(f"Failed to create columns for Table '{table_name}'.")
                return  # Exit if columns creation failed

        # Step 2: Update each Column with 'data_type'
        for col_name in column_names:
            col_data_type = column_type_mapping.get(col_name, 'string')
            logging.info(f"Updating data_type for Column '{col_name}' to '{col_data_type}'.")
            print(f"Updating data_type for Column '{col_name}' to '{col_data_type}'.")

            # Build the qualified name of the Column
            column_qualified_name = f"{table_asset.qualified_name}/{col_name}"

            try:
                # Retrieve the existing column
                column = client.get_asset_by_qualified_name(asset_type=Column, qualified_name=column_qualified_name)
                if column:
                    logging.info(f"Retrieved Column '{column.name}' with current data type '{column.data_type}'.")
                    print(f"Retrieved Column '{column.name}' with current data type '{column.data_type}'.")

                    # Update the data_type
                    column.data_type = col_data_type

                    # Save the updated column
                    update_response = client.asset.save(column)
                    if update_response:
                        print(f"Updated Column '{col_name}' with data_type '{col_data_type}'.")
                        logging.info(f"Updated Column '{col_name}' with data_type '{col_data_type}'.")
                    else:
                        print(f"Failed to update data_type for Column '{col_name}'.")
                        logging.error(f"Failed to update data_type for Column '{col_name}'.")
                else:
                    print(f"Column with qualified name '{column_qualified_name}' not found.")
                    logging.error(f"Column with qualified name '{column_qualified_name}' not found.")
            except Exception as e:
                logging.error(f"An error occurred while updating the column '{col_name}': {e}")
                print(f"An error occurred while updating the column '{col_name}': {e}")

    except Exception as e:
        logging.error(f"Error processing CSV file '{csv_file_key}': {e}")
        print(f"Error processing CSV file '{csv_file_key}': {e}")
        raise

def main():
    try:
        # Ensure the S3 connection exists
        connection_qualified_name = create_s3_connection()
        if not connection_qualified_name:
            print("Failed to create or retrieve S3 Connection.")
            logging.error("Failed to create or retrieve S3 Connection.")
            return

        # Create or retrieve Database
        database_asset = create_database(connection_qualified_name)
        if not database_asset:
            print("Failed to create or retrieve Database.")
            logging.error("Failed to create or retrieve Database.")
            return

        # Create or retrieve Schema
        schema_asset = create_schema(database_asset.qualified_name)
        if not schema_asset:
            print("Failed to create or retrieve Schema.")
            logging.error("Failed to create or retrieve Schema.")
            return

        # List CSV files from S3 bucket
        csv_files = config.S3_OBJECT_NAMES  # From config.py
        if not csv_files:
            print(f"No CSV files specified in 'S3_OBJECT_NAMES'.")
            logging.warning(f"No CSV files specified in 'S3_OBJECT_NAMES'.")
            return

        # Process all CSV files
        for csv_file_key in csv_files:
            extract_and_create_columns(csv_file_key, schema_asset.qualified_name)

        print("\nSchema extraction and update completed.")
        logging.info("Schema extraction and update completed.")
        print("\nTo view the assets in Atlan:")
        print(f"- Connection: {config.AWS_CONNECTION_NAME}")
        print(f"- Database: {database_asset.name}")
        print(f"- Schema: {schema_asset.name}")
        print("- Tables and Columns are under the Schema.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
