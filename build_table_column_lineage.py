import logging
import hashlib
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection, Table, Column, Process, ColumnProcess
from pyatlan.model.fluent_search import FluentSearch
import config

# Configure logging
logging.basicConfig(
    filename=config.LOG_FILE,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=getattr(logging, config.LOG_LEVEL.upper())
)

# Initialize Atlan client
client = AtlanClient(
    base_url=config.BASE_URL,
    api_key=config.API_TOKEN
)

def get_connection_qualified_name(connection_name):
    """Retrieve the qualified name of the specified connection."""
    try:
        search_request = (
            FluentSearch()
            .where(FluentSearch.active_assets())
            .where(FluentSearch.asset_type(Connection))
            .where(Connection.NAME.eq(connection_name))
            .page_size(1)
            .to_request()
        )
        connections = client.asset.search(search_request)
        for connection in connections.current_page():
            return connection.qualified_name
        return None
    except Exception as e:
        logging.error(f"Error retrieving qualified name for connection '{connection_name}': {e}")
        raise

def search_tables_in_connection(connection_name):
    """
    Searches for all table assets in Atlan under a given connection.
    :param connection_name: Name of the connection.
    :return: A list of Table assets.
    """
    try:
        # First, get the connection qualified name
        connection_qualified_name = get_connection_qualified_name(connection_name)
        if not connection_qualified_name:
            logging.warning(f"Connection '{connection_name}' not found.")
            return []

        # Search for tables under this connection
        search = (
            FluentSearch()
            .where(Table.CONNECTION_QUALIFIED_NAME.eq(connection_qualified_name))
            .where(FluentSearch.active_assets())
            .page_size(1000)
        ).to_request()

        search_results = client.asset.search(search)
        tables = [asset for asset in search_results.current_page() if isinstance(asset, Table)]
        logging.info(f"Found {len(tables)} tables under connection '{connection_name}'.")
        print(f"\nFound {len(tables)} tables under connection '{connection_name}':")
        for table in tables:
            print(f" - Table Name: {table.name}, GUID: {table.guid}, Qualified Name: {table.qualified_name}")
        return tables
    except Exception as e:
        logging.error(f"Error occurred while searching for tables under connection '{connection_name}': {e}")
        return []

def confirm_proceed(message):
    """Prompt the user to confirm proceeding."""
    while True:
        user_input = input(f"{message} (Y/n): ").strip().lower()
        if user_input in ['y', 'yes', '']:
            return True
        elif user_input in ['n', 'no']:
            return False
        else:
            print("Invalid input. Please enter 'Y' or 'n'.")

def get_process_by_qualified_name(qualified_name):
    """Retrieve a Process asset by its qualified_name."""
    try:
        search_request = (
            FluentSearch()
            .where(FluentSearch.active_assets())
            .where(FluentSearch.asset_type(Process))
            .where(Process.QUALIFIED_NAME.eq(qualified_name))
            .page_size(1)
            .to_request()
        )
        search_results = client.asset.search(search_request)
        processes = [asset for asset in search_results.current_page() if isinstance(asset, Process)]
        if processes:
            return processes[0]
        else:
            return None
    except Exception as e:
        logging.error(f"Error occurred while searching for Process with qualified_name '{qualified_name}': {e}")
        return None

def get_column_process_by_qualified_name(qualified_name):
    """Retrieve a ColumnProcess asset by its qualified_name."""
    try:
        search_request = (
            FluentSearch()
            .where(FluentSearch.active_assets())
            .where(FluentSearch.asset_type(ColumnProcess))
            .where(ColumnProcess.QUALIFIED_NAME.eq(qualified_name))
            .page_size(1)
            .to_request()
        )
        search_results = client.asset.search(search_request)
        column_processes = [asset for asset in search_results.current_page() if isinstance(asset, ColumnProcess)]
        if column_processes:
            return column_processes[0]
        else:
            return None
    except Exception as e:
        logging.error(f"Error occurred while searching for ColumnProcess with qualified_name '{qualified_name}': {e}")
        return None

def generate_process_id(source_qualified_name, target_qualified_name):
    """Generate a unique process_id based on source and target qualified names."""
    combined = f"{source_qualified_name}|{target_qualified_name}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()

def set_table_lineage(source_table: Table, target_table: Table, connection_qualified_name: str, connection_name: str):
    """
    Sets lineage between two tables in Atlan using Process entities.
    :param source_table: Source Table asset.
    :param target_table: Target Table asset.
    :param connection_qualified_name: Qualified name of the connection to associate with the process.
    :param connection_name: Name of the connection.
    """
    try:
        process_name = f"{source_table.name} -> {target_table.name}"
        process_id = f"process_{generate_process_id(source_table.qualified_name, target_table.qualified_name)}"
        # Construct the expected qualified_name based on default pattern
        process_qualified_name = f"{connection_qualified_name}/{process_id}"

        # Check if Process with this qualified_name already exists
        existing_process = get_process_by_qualified_name(process_qualified_name)
        if existing_process:
            logging.info(f"Process with qualified_name '{process_qualified_name}' already exists. Using existing Process.")
            print(f"Process lineage from '{source_table.name}' (Connection: {connection_name}) to '{target_table.name}' (Connection: {connection_name}) already exists.")
            return existing_process.guid, existing_process.qualified_name

        # Create Process entity
        process = Process.creator(
            name=process_name,
            connection_qualified_name=connection_qualified_name,
            process_id=process_id,
            inputs=[Table.ref_by_guid(source_table.guid)],
            outputs=[Table.ref_by_guid(target_table.guid)],
        )
        # Set additional attributes
        process.description = f"Lineage from {source_table.name} to {target_table.name}"
        # Save the Process entity to establish lineage
        response = client.asset.save(process)
        if response and response.assets_created(asset_type=Process):
            # Extract the GUID and qualified_name of the created Process
            processes = response.assets_created(asset_type=Process)
            process_guid = processes[0].guid
            process_qualified_name = processes[0].qualified_name
            logging.info(f"Successfully created lineage with Process GUID '{process_guid}'.")
            print(f"Successfully created lineage from '{source_table.name}' (Connection: {connection_name}) to '{target_table.name}' (Connection: {connection_name}).")
            return process_guid, process_qualified_name
        else:
            logging.error(f"Failed to create lineage from '{source_table.name}' to '{target_table.name}'.")
            print(f"Failed to create lineage from '{source_table.name}' to '{target_table.name}'.")
            return None, None
    except Exception as e:
        logging.error(f"Error occurred while setting table lineage: {e}")
        print(f"Error occurred while setting table lineage: {e}")
        return None, None

def set_column_lineage(source_column: Column, target_column: Column, parent_process_guid: str, parent_process_qualified_name: str):
    """
    Sets lineage between two columns in Atlan using ColumnProcess entities.
    :param source_column: Source Column asset.
    :param target_column: Target Column asset.
    :param parent_process_guid: GUID of the parent Process.
    :param parent_process_qualified_name: Qualified name of the parent Process.
    """
    try:
        column_process_name = f"{source_column.name} -> {target_column.name}"
        process_id = f"column_process_{generate_process_id(source_column.qualified_name, target_column.qualified_name)}"
        # Construct the expected qualified_name
        column_process_qualified_name = f"{parent_process_qualified_name}/{process_id}"

        # Check if ColumnProcess with this qualified_name already exists
        existing_column_process = get_column_process_by_qualified_name(column_process_qualified_name)
        if existing_column_process:
            logging.info(f"ColumnProcess with qualified_name '{column_process_qualified_name}' already exists. Using existing ColumnProcess.")
            print(f"Column lineage from '{source_column.name}' to '{target_column.name}' already exists.")
            return

        # Create ColumnProcess entity
        column_process = ColumnProcess.creator(
            name=column_process_name,
            connection_qualified_name=parent_process_qualified_name,  # Use parent process's qualified_name
            process_id=process_id,
            inputs=[Column.ref_by_guid(source_column.guid)],
            outputs=[Column.ref_by_guid(target_column.guid)],
            parent=Process.ref_by_guid(parent_process_guid),
        )
        # Set additional attributes
        column_process.description = f"Lineage from {source_column.name} to {target_column.name}"
        # Save the ColumnProcess entity to establish lineage
        response = client.asset.save(column_process)
        if response and response.assets_created(asset_type=ColumnProcess):
            logging.info(f"Successfully created column lineage from '{source_column.name}' to '{target_column.name}'.")
            print(f"Successfully created column lineage from '{source_column.name}' to '{target_column.name}'.")
        else:
            logging.error(f"Failed to create column lineage from '{source_column.name}' to '{target_column.name}'.")
            print(f"Failed to create column lineage from '{source_column.name}' to '{target_column.name}'.")
    except Exception as e:
        logging.error(f"Error occurred while setting column lineage: {e}")
        print(f"Error occurred while setting column lineage: {e}")

def get_columns_for_table(table: Table, connection_name: str):
    """
    Retrieves all columns for a given table.
    :param table: Table asset.
    :param connection_name: Name of the connection the table belongs to.
    :return: List of Column assets.
    """
    try:
        search = (
            FluentSearch()
            .where(FluentSearch.active_assets())
            .where(FluentSearch.asset_type(Column))
            .where(Column.TABLE_QUALIFIED_NAME.eq(table.qualified_name))
            .page_size(100)
        ).to_request()

        search_results = client.asset.search(search)
        columns = [asset for asset in search_results.current_page() if isinstance(asset, Column)]
        logging.info(f"Found {len(columns)} columns for table '{table.name}' in connection '{connection_name}'.")
        print(f"Found {len(columns)} columns for table '{table.name}' in connection '{connection_name}':")
        for column in columns:
            print(f" - Column Name: {column.name}, GUID: {column.guid}, Qualified Name: {column.qualified_name}")
        return columns
    except Exception as e:
        logging.error(f"Error occurred while retrieving columns for table '{table.name}': {e}")
        return []

def main():
    # Get connection qualified names
    postgres_connection_qualified_name = get_connection_qualified_name(config.POSTGRES_CONNECTION_NAME)
    s3_connection_qualified_name = get_connection_qualified_name(config.AWS_CONNECTION_NAME)
    snowflake_connection_qualified_name = get_connection_qualified_name(config.SNOWFLAKE_CONNECTION_NAME)

    # Get tables from Postgres connection
    postgres_tables = search_tables_in_connection(config.POSTGRES_CONNECTION_NAME)
    proceed = confirm_proceed("Proceed with Postgres tables?")
    if not proceed:
        print("Aborting.")
        return

    # Get tables from S3 connection
    s3_tables = search_tables_in_connection(config.AWS_CONNECTION_NAME)
    proceed = confirm_proceed("Proceed with S3 tables?")
    if not proceed:
        print("Aborting.")
        return

    # Get tables from Snowflake connection
    snowflake_tables = search_tables_in_connection(config.SNOWFLAKE_CONNECTION_NAME)
    proceed = confirm_proceed("Proceed with Snowflake tables?")
    if not proceed:
        print("Aborting.")
        return

    # Build dictionaries to map table names to Table assets (case-insensitive)
    postgres_table_dict = {table.name.lower(): table for table in postgres_tables}
    s3_table_dict = {table.name.lower(): table for table in s3_tables}
    snowflake_table_dict = {table.name.lower(): table for table in snowflake_tables}

    # Establish lineage from Postgres to S3 tables
    for table_name_lower, postgres_table in postgres_table_dict.items():
        s3_table = s3_table_dict.get(table_name_lower)
        if s3_table:
            print(f"\nMatched Postgres table '{postgres_table.name}' with S3 table '{s3_table.name}'.")
            print("Establishing table lineage...")
            # Set table-level lineage using Process entity
            process_guid, process_qualified_name = set_table_lineage(
                postgres_table, s3_table, connection_qualified_name=s3_connection_qualified_name, connection_name=config.AWS_CONNECTION_NAME
            )

            if process_guid:
                # Get columns for both tables
                postgres_columns = get_columns_for_table(postgres_table, config.POSTGRES_CONNECTION_NAME)
                s3_columns = get_columns_for_table(s3_table, config.AWS_CONNECTION_NAME)

                # Map columns by name (case-insensitive)
                postgres_column_dict = {col.name.lower(): col for col in postgres_columns}
                s3_column_dict = {col.name.lower(): col for col in s3_columns}

                # Establish column-level lineage
                for col_name_lower, postgres_column in postgres_column_dict.items():
                    s3_column = s3_column_dict.get(col_name_lower)
                    if s3_column:
                        print(f" - Matched Postgres column '{postgres_column.name}' with S3 column '{s3_column.name}'.")
                        print(f"Establishing lineage between columns '{postgres_column.name}' (Postgres) and '{s3_column.name}' (S3)...")
                        set_column_lineage(
                            postgres_column,
                            s3_column,
                            parent_process_guid=process_guid,
                            parent_process_qualified_name=process_qualified_name,
                        )
                    else:
                        logging.warning(f"No matching S3 column found for Postgres column '{postgres_column.name}' in table '{postgres_table.name}'.")
            else:
                print("Failed to create table lineage; skipping column lineage.")
        else:
            logging.warning(f"No matching S3 table found for Postgres table '{postgres_table.name}'.")

    # Establish lineage from S3 to Snowflake tables
    for table_name_lower, s3_table in s3_table_dict.items():
        snowflake_table = snowflake_table_dict.get(table_name_lower)
        if snowflake_table:
            print(f"\nMatched S3 table '{s3_table.name}' with Snowflake table '{snowflake_table.name}'.")
            print("Establishing table lineage...")
            # Set table-level lineage using Process entity
            process_guid, process_qualified_name = set_table_lineage(
                s3_table, snowflake_table, connection_qualified_name=snowflake_connection_qualified_name, connection_name=config.SNOWFLAKE_CONNECTION_NAME
            )

            if process_guid:
                # Get columns for both tables
                s3_columns = get_columns_for_table(s3_table, config.AWS_CONNECTION_NAME)
                snowflake_columns = get_columns_for_table(snowflake_table, config.SNOWFLAKE_CONNECTION_NAME)

                # Map columns by name (case-insensitive)
                s3_column_dict = {col.name.lower(): col for col in s3_columns}
                snowflake_column_dict = {col.name.lower(): col for col in snowflake_columns}

                # Establish column-level lineage
                for col_name_lower, s3_column in s3_column_dict.items():
                    snowflake_column = snowflake_column_dict.get(col_name_lower)
                    if snowflake_column:
                        print(f" - Matched S3 column '{s3_column.name}' with Snowflake column '{snowflake_column.name}'.")
                        print(f"Establishing lineage between columns '{s3_column.name}' (S3) and '{snowflake_column.name}' (Snowflake)...")
                        set_column_lineage(
                            s3_column,
                            snowflake_column,
                            parent_process_guid=process_guid,
                            parent_process_qualified_name=process_qualified_name,
                        )
                    else:
                        logging.warning(f"No matching Snowflake column found for S3 column '{s3_column.name}' in table '{s3_table.name}'.")
            else:
                print("Failed to create table lineage; skipping column lineage.")
        else:
            logging.warning(f"No matching Snowflake table found for S3 table '{s3_table.name}'.")

    print("\nLineage establishment completed.")
    logging.info("Lineage establishment completed.")

if __name__ == "__main__":
    main()
