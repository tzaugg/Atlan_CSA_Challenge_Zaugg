import logging
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
            print(f" - Table Name: {table.name}, GUID: {table.guid}")
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

def set_table_lineage(source_table: Table, target_table: Table, connection_qualified_name: str):
    """
    Sets lineage between two tables in Atlan using Process entities.
    :param source_table: Source Table asset.
    :param target_table: Target Table asset.
    :param connection_qualified_name: Qualified name of the connection to associate with the process.
    """
    try:
        process_name = f"{source_table.name} -> {target_table.name}"
        process_id = f"process_{source_table.guid}_{target_table.guid}"

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
        response = asset.save(process)
        if response:
            # Extract the GUID of the created Process
            processes = response.assets_created(asset_type=Process)
            if processes:
                process_guid = processes[0].guid
                logging.info(f"Successfully created lineage with Process GUID '{process_guid}'.")
                print(f"Successfully created lineage from '{source_table.name}' to '{target_table.name}'.")
                return process_guid
            else:
                logging.error("No Process asset returned in response.")
                return None
        else:
            logging.error(f"Failed to create lineage from '{source_table.qualified_name}' to '{target_table.qualified_name}'.")
            print(f"Failed to create lineage from '{source_table.name}' to '{target_table.name}'.")
            return None
    except Exception as e:
        logging.error(f"Error occurred while setting table lineage: {e}")
        print(f"Error occurred while setting table lineage: {e}")
        return None

def set_column_lineage(source_column: Column, target_column: Column, parent_process_guid: str, connection_qualified_name: str):
    """
    Sets lineage between two columns in Atlan using ColumnProcess entities.
    :param source_column: Source Column asset.
    :param target_column: Target Column asset.
    :param parent_process_guid: GUID of the parent Process.
    :param connection_qualified_name: Qualified name of the connection to associate with the process.
    """
    try:
        column_process_name = f"{source_column.name} -> {target_column.name}"
        column_process_id = f"column_process_{source_column.guid}_{target_column.guid}"

        # Create ColumnProcess entity
        column_process = ColumnProcess.creator(
            name=column_process_name,
            connection_qualified_name=connection_qualified_name,
            process_id=column_process_id,
            inputs=[Column.ref_by_guid(source_column.guid)],
            outputs=[Column.ref_by_guid(target_column.guid)],
            parent=Process.ref_by_guid(parent_process_guid),
        )

        # Set additional attributes
        column_process.description = f"Lineage from {source_column.name} to {target_column.name}"
        # Save the ColumnProcess entity to establish lineage
        response = asset.save(column_process)
        if response:
            logging.info(f"Successfully created column lineage from '{source_column.qualified_name}' to '{target_column.qualified_name}'.")
            print(f"Successfully created column lineage from '{source_column.name}' to '{target_column.name}'.")
        else:
            logging.error(f"Failed to create column lineage from '{source_column.qualified_name}' to '{target_column.qualified_name}'.")
            print(f"Failed to create column lineage from '{source_column.name}' to '{target_column.name}'.")
    except Exception as e:
        logging.error(f"Error occurred while setting column lineage: {e}")
        print(f"Error occurred while setting column lineage: {e}")

def get_columns_for_table(table: Table):
    """
    Retrieves all columns for a given table.
    :param table: Table asset.
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
        logging.info(f"Found {len(columns)} columns for table '{table.name}'.")
        print(f"Found {len(columns)} columns for table '{table.name}':")
        for column in columns:
            print(f" - Column Name: {column.name}, GUID: {column.guid}")
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

    # Build dictionaries to map table names to Table assets
    postgres_table_dict = {table.name: table for table in postgres_tables}
    s3_table_dict = {table.name: table for table in s3_tables}
    snowflake_table_dict = {table.name: table for table in snowflake_tables}

    # Establish lineage from Postgres to S3 tables
    for table_name, postgres_table in postgres_table_dict.items():
        s3_table = s3_table_dict.get(table_name)
        if s3_table:
            print(f"\nMatched Postgres table '{table_name}' with S3 table '{s3_table.name}'.")
            print("Establishing table lineage...")
            # Set table-level lineage using Process entity
            process_guid = set_table_lineage(
                postgres_table, s3_table, connection_qualified_name=s3_connection_qualified_name
            )

            if process_guid:
                # Get columns for both tables
                postgres_columns = get_columns_for_table(postgres_table)
                s3_columns = get_columns_for_table(s3_table)

                # Map columns by name
                postgres_column_dict = {col.name: col for col in postgres_columns}
                s3_column_dict = {col.name: col for col in s3_columns}

                # Establish column-level lineage
                for col_name, postgres_column in postgres_column_dict.items():
                    s3_column = s3_column_dict.get(col_name)
                    if s3_column:
                        print(f" - Matched Postgres column '{col_name}' with S3 column '{s3_column.name}'.")
                        print(f"Establishing lineage between columns '{col_name}'...")
                        set_column_lineage(
                            postgres_column,
                            s3_column,
                            parent_process_guid=process_guid,
                            connection_qualified_name=s3_connection_qualified_name,
                        )
                    else:
                        logging.warning(f"No matching S3 column found for Postgres column '{col_name}' in table '{table_name}'.")
            else:
                print("Failed to create table lineage; skipping column lineage.")
        else:
            logging.warning(f"No matching S3 table found for Postgres table '{table_name}'.")

    # Establish lineage from S3 to Snowflake tables
    for table_name, s3_table in s3_table_dict.items():
        snowflake_table = snowflake_table_dict.get(table_name)
        if snowflake_table:
            print(f"\nMatched S3 table '{table_name}' with Snowflake table '{snowflake_table.name}'.")
            print("Establishing table lineage...")
            # Set table-level lineage using Process entity
            process_guid = set_table_lineage(
                s3_table, snowflake_table, connection_qualified_name=snowflake_connection_qualified_name
            )

            if process_guid:
                # Get columns for both tables
                s3_columns = get_columns_for_table(s3_table)
                snowflake_columns = get_columns_for_table(snowflake_table)

                # Map columns by name
                s3_column_dict = {col.name: col for col in s3_columns}
                snowflake_column_dict = {col.name: col for col in snowflake_columns}

                # Establish column-level lineage
                for col_name, s3_column in s3_column_dict.items():
                    snowflake_column = snowflake_column_dict.get(col_name)
                    if snowflake_column:
                        print(f" - Matched S3 column '{col_name}' with Snowflake column '{snowflake_column.name}'.")
                        print(f"Establishing lineage between columns '{col_name}'...")
                        set_column_lineage(
                            s3_column,
                            snowflake_column,
                            parent_process_guid=process_guid,
                            connection_qualified_name=snowflake_connection_qualified_name,
                        )
                    else:
                        logging.warning(f"No matching Snowflake column found for S3 column '{col_name}' in table '{table_name}'.")
            else:
                print("Failed to create table lineage; skipping column lineage.")
        else:
            logging.warning(f"No matching Snowflake table found for S3 table '{table_name}'.")

    print("\nLineage establishment completed.")
    logging.info("Lineage establishment completed.")

if __name__ == "__main__":
    main()
