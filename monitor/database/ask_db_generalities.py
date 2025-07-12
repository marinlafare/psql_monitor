# database/ask_db_generalities.py
import asyncpg
import os
import asyncio

from monitor.constants import *


async def print_all_databases_and_tables():
    """
    Connects to a default PostgreSQL database to list all databases,
    then connects to each database to list its tables.
    """

    # Check if the variables loaded from .env are not None
    if not all([USER, PASSWORD, HOST, PORT]):
        print("Error: Missing one or more database connection parameters in .env file.")
        return

    # Construct the base connection string for the admin database (e.g., 'postgres')
    # Use the variables that were loaded from the .env file
    admin_conn_string = (
        f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres"
    )

    print("--- Discovering Databases ---")
    try:
        # Query to get all non-template database names
        db_names = await open_async_request(
            admin_conn_string,
            "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1');",
            fetch_as_dict=True
        )
        
        if not db_names:
            print("No user-defined databases found.")
            return

        print(f"Found {len(db_names)} user-defined databases.")
        
        for db_info in db_names:
            db_name = db_info['datname']
            print(f"\n--- Processing Database: {db_name} ---")

            # Construct the connection string for the current database
            # Use the variables that were loaded from the .env file
            current_db_conn_string = (
                f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{db_name}"
            )

            try:
                # Query to get all tables in the 'public' schema for the current database
                tables = await open_async_request(
                    current_db_conn_string,
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';",
                    fetch_as_dict=True
                )

                if tables:
                    print(f"Tables in '{db_name}' (public schema):")
                    for table in tables:
                        print(f"  - {table['table_name']}")
                else:
                    print(f"No tables found in '{db_name}' (public schema).")

            except Exception as e:
                print(f"  Error accessing tables in database '{db_name}': {e}")

    except Exception as e:
        print(f"Error discovering databases: {e}")


async def get_db_connection_strings_and_tables_dict() -> dict:
    """
    Returns a dictionary where keys are database names, and values are
    dictionaries containing the 'conn' (connection string) and 'tables' (list of table names).
    Only includes user-defined databases.

    Returns:
        A dictionary like {'db_name1': {'conn': 'postgresql://.../db1', 'tables': ['table1', 'table2']},
                           'db_name2': {'conn': 'postgresql://.../db2', 'tables': ['tableA', 'tableB']}}
    """
    db_structure_with_conn_info = {}

    if not all([USER, PASSWORD, HOST, PORT]):
        print("Error: Missing one or more database connection parameters in .env file. Cannot fetch database structure.")
        return db_structure_with_conn_info

    admin_conn_string = (
        f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres"
    )

    try:
        # Get all non-template database names
        db_names = await open_async_request(
            admin_conn_string,
            "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1');",
            fetch_as_dict=True
        )
        
        if not db_names:
            print("No user-defined databases found to build structure.")
            return db_structure_with_conn_info

        for db_info in db_names:
            db_name = db_info['datname']
            # Construct the full connection string for the current database
            current_db_conn_string = (
                f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{db_name}"
            )

            tables_info = {} # This will be the dictionary for tables
            try:
                # First, get all table names in the 'public' schema for the current database
                tables_data = await open_async_request(
                    current_db_conn_string,
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';",
                    fetch_as_dict=True
                )
                
                # For each table, get its column names
                for table_row in tables_data:
                    table_name = table_row['table_name']
                    column_names_list = []
                    try:
                        columns_data = await open_async_request(
                            current_db_conn_string,
                            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position;",
                            params=(table_name,), # Pass table_name as a tuple parameter
                            fetch_as_dict=True
                        )
                        column_names_list = [col['column_name'] for col in columns_data]
                    except Exception as col_e:
                        print(f"Warning: Could not access columns for table '{table_name}' in database '{db_name}': {col_e}")
                    
                    tables_info[table_name] = column_names_list

            except Exception as e:
                print(f"Warning: Could not access tables for database '{db_name}' using connection string '{current_db_conn_string}': {e}")
                # tables_info remains empty or partially filled if an error occurs

            # Populate the dictionary with the new structure
            db_structure_with_conn_info[db_name] = {
                'conn': current_db_conn_string,
                'tables': tables_info # Now a dictionary of table_name -> [column_names]
            }

    except Exception as e:
        print(f"Error building database connection string structure: {e}")

    return db_structure_with_conn_info

async def get_dbs_general_size() -> dict:
    """
    Returns the total size of all user-defined databases on the PostgreSQL server in gigabytes (GB).

    Returns:
        A dictionary with a single key 'total_size_gb' and its value,
        or an empty dictionary if an error occurs.
    """
    if not all([USER, PASSWORD, HOST, PORT]):
        print("Error: Missing one or more database connection parameters in .env file. Cannot fetch general size.")
        return {}

    admin_conn_string = (
        f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres"
    )

    total_size_bytes = 0
    try:
        # Get all non-template database names
        db_names = await open_async_request(
            admin_conn_string,
            "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1');",
            fetch_as_dict=True
        )
        
        if not db_names:
            print("No user-defined databases found to calculate general size.")
            return {'total_size_gb': 0.0} # Return 0.0 GB if no databases

        for db_info in db_names:
            db_name = db_info['datname']
            try:
                # Query the size of each database in bytes
                size_bytes_result = await open_async_request(
                    admin_conn_string, # Can query pg_database_size from any database, but admin is fine
                    "SELECT pg_database_size($1);",
                    params=(db_name,),
                    fetch_as_dict=False # Returns a single value
                )
                if size_bytes_result and size_bytes_result[0] is not None:
                    total_size_bytes += size_bytes_result[0][0] # size_bytes_result is a list of lists/tuples
            except Exception as e:
                print(f"Warning: Could not get size for database '{db_name}': {e}")

    except Exception as e:
        print(f"Error calculating general database size: {e}")
        return {}

    # Convert total bytes to gigabytes and round to 2 decimal places
    total_size_gb = round(total_size_bytes / (1024 ** 3), 2)
    return  f"{total_size_gb} GB"


async def get_one_db_size(db_name: str) -> dict:
    """
    Returns the size of a specific database in gigabytes (GB) with 2 decimal places.

    Args:
        db_name: The name of the database to get the size for.

    Returns:
        A dictionary with a single key 'size_gb' and its value,
        or an empty dictionary if an error occurs or database not found.
    """
    if not all([USER, PASSWORD, HOST, PORT]):
        print("Error: Missing one or more database connection parameters in .env file. Cannot fetch database size.")
        return {}

    # Construct the connection string for the specific database
    specific_db_conn_string = (
        f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{db_name}"
    )

    try:
        # Query the size of the current database in bytes
        size_bytes_result = await open_async_request(
            specific_db_conn_string,
            "SELECT pg_database_size(current_database());",
            fetch_as_dict=False # Returns a single value
        )
        
        if size_bytes_result and size_bytes_result[0] is not None:
            size_bytes = size_bytes_result[0][0] # size_bytes_result is a list of lists/tuples
            # Convert to gigabytes and round to 2 decimal places
            size_gb = round(size_bytes / (1024 ** 3), 2)
            
            return f"{size_gb} GB"
        else:
            print(f"Warning: Database '{db_name}' not found or size could not be retrieved.")
            return {}
    except Exception as e:
        print(f"Error getting size for database '{db_name}': {e}")
        return {}


async def get_table_size(db_name: str, table_name: str) -> float | None:
    """
    Connects to a specified PostgreSQL database and returns the total size of a given table
    (including indexes and TOAST data) in megabytes.

    Args:
        db_name (str): The name of the database to connect to.
        table_name (str): The name of the table whose size is to be retrieved.

    Returns:
        float | None: The size of the table in megabytes, or None if the table
                      or database is not found, or an error occurs.
    """
    conn = None
    try:
        conn_string = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{db_name}"
        conn = await asyncpg.connect(conn_string)
        query = f"SELECT pg_total_relation_size('{table_name}');"
        size_bytes = await conn.fetchval(query)
        if size_bytes is None:
            print(f"Warning: Table '{table_name}' not found in database '{db_name}' or size could not be retrieved.")
            return None
        else:
            size_gb = size_bytes / (1024 * 1024 * 1024)
            return f"{size_gb:.3f} GB"

    except asyncpg.exceptions.PostgresError as e:
        print(f"A PostgreSQL specific error occurred: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while fetching table size for '{table_name}' in '{db_name}': {e}")
        return None
    finally:
        if conn:
            await conn.close()