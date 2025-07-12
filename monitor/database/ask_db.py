# database/ask_db
import asyncpg
import os
import asyncio
from dotenv import load_dotenv
from constants import *


async def open_async_request(db_str: str,
                             sql_question: str,
                             params: tuple = None,
                             fetch_as_dict: bool = False):
    """
    Executes an asynchronous SQL query, optionally with parameters, and fetches results.
    Establishes and closes an asyncpg connection internally for each request.

    Args:
        db_str: The full PostgreSQL connection string for the target database.
        sql_question: The SQL query string.
        params: A tuple of parameters to pass to the query (asyncpg uses positional parameters like $1, $2).
        fetch_as_dict: If True, fetches results as a list of dictionaries.
                       Otherwise, returns a list of asyncpg.Record objects (which behave like tuples).

    Returns:
        A list of query results (dictionaries or asyncpg.Record objects).
    """
    conn = None
    try:
        # Replace 'postgresql+asyncpg' with 'postgresql' for direct asyncpg.connect.
        actual_conn_string = db_str.replace("postgresql+asyncpg://", "postgresql://")
        
        conn = await asyncpg.connect(actual_conn_string)

        if params:
            rows = await conn.fetch(sql_question, *params)
        else:
            rows = await conn.fetch(sql_question)
        
        if fetch_as_dict:
            return [dict(row) for row in rows]
        else:
            return rows
    except Exception as e:
        print(f"Error in open_async_request: {e}")
        raise # Re-raise to propagate the error
    finally:
        if conn:
            await conn.close()


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
        f"postgresql+asyncpg://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres"
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
                f"postgresql+asyncpg://{USER}:{PASSWORD}@{HOST}:{PORT}/{db_name}"
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



# async def get_db_connection_strings_and_tables_dict() -> dict:
#     """
#     Returns a dictionary where keys are database names, and values are
#     dictionaries containing the 'conn' (connection string) and 'tables' (list of table names).
#     Only includes user-defined databases.

#     Returns:
#         A dictionary like {'db_name1': {'conn': 'postgresql+asyncpg://.../db1', 'tables': ['table1', 'table2']},
#                            'db_name2': {'conn': 'postgresql+asyncpg://.../db2', 'tables': ['tableA', 'tableB']}}
#     """
#     db_structure_with_conn_info = {}

#     if not all([USER, PASSWORD, HOST, PORT]):
#         print("Error: Missing one or more database connection parameters in .env file. Cannot fetch database structure.")
#         return db_structure_with_conn_info

#     admin_conn_string = (
#         f"postgresql+asyncpg://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres"
#     )

#     try:
#         # Get all non-template database names
#         db_names = await open_async_request(
#             admin_conn_string,
#             "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1');",
#             fetch_as_dict=True
#         )
        
#         if not db_names:
#             print("No user-defined databases found to build structure.")
#             return db_structure_with_conn_info

#         for db_info in db_names:
#             db_name = db_info['datname']
#             # Construct the full connection string for the current database
#             current_db_conn_string = (
#                 f"postgresql+asyncpg://{USER}:{PASSWORD}@{HOST}:{PORT}/{db_name}"
#             )

#             tables_list = []
#             try:
#                 # Get tables in the 'public' schema for the current database
#                 tables_data = await open_async_request(
#                     current_db_conn_string,
#                     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';",
#                     fetch_as_dict=True
#                 )
#                 # Extract just the table names into a list
#                 tables_list = [table['table_name'] for table in tables_data]
#             except Exception as e:
#                 print(f"Warning: Could not access tables for database '{db_name}' using connection string '{current_db_conn_string}': {e}")
#                 # tables_list remains empty if an error occurs

#             # Populate the dictionary with the new structure
#             db_structure_with_conn_info[db_name] = {
#                 'conn': current_db_conn_string,
#                 'tables': tables_list
#             }

#     except Exception as e:
#         print(f"Error building database connection string structure: {e}")

#     return db_structure_with_conn_info

async def get_db_connection_strings_and_tables_dict() -> dict:
    """
    Returns a dictionary where keys are database names, and values are
    dictionaries containing the 'conn' (connection string) and 'tables' (list of table names).
    Only includes user-defined databases.

    Returns:
        A dictionary like {'db_name1': {'conn': 'postgresql+asyncpg://.../db1', 'tables': ['table1', 'table2']},
                           'db_name2': {'conn': 'postgresql+asyncpg://.../db2', 'tables': ['tableA', 'tableB']}}
    """
    db_structure_with_conn_info = {}

    if not all([USER, PASSWORD, HOST, PORT]):
        print("Error: Missing one or more database connection parameters in .env file. Cannot fetch database structure.")
        return db_structure_with_conn_info

    admin_conn_string = (
        f"postgresql+asyncpg://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres"
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
                f"postgresql+asyncpg://{USER}:{PASSWORD}@{HOST}:{PORT}/{db_name}"
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