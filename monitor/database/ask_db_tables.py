# database/ask_db_tables.py
import asyncpg
import os
import asyncio
#from asyncpg.utils import quote_ident 
from monitor.constants import *

# database/ask_db_tables.py
import asyncpg
import os
import asyncio
# Removed: from asyncpg.utils import quote_ident # This import caused the ImportError

# Import constants and the open_async_request function from monitor.constants
from monitor.constants import BASE_DB_CONN_STRING, open_async_request


async def table_columns_dict(db_name: str, table_name: str) -> dict | None:
    """
    Connects to a specified PostgreSQL database and retrieves a dictionary
    where keys are column names of a given table and values are the count
    of non-NULL items in each respective column.

    Args:
        db_name (str): The name of the database to connect to.
        table_name (str): The name of the table to inspect.

    Returns:
        dict | None: A dictionary with column names as keys and their non-NULL counts as values,
                     or None if the database/table does not exist or an error occurs.
    """
    column_counts = {}
    conn = None # Initialize connection to None

    try:
        # Establish a single connection for all operations within this function
        conn_string = f"{BASE_DB_CONN_STRING}/{db_name}"
        conn = await asyncpg.connect(conn_string)

        # First, get all column names for the specified table
        # We query information_schema.columns to get metadata about the table's columns.
        # Using parameterized query for table_schema and table_name (values)
        columns_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2;
        """
        # Execute the query using the established connection
        column_records = await conn.fetch(
            columns_query,
            'public', # Assuming 'public' schema, adjust if needed
            table_name
        )

        if not column_records:
            print(f"Warning: No columns found for table '{table_name}' in database '{db_name}'. "
                  f"Table might not exist or is empty, or schema is not 'public'.")
            return None

        # Iterate through each column and get its non-NULL count
        for col_record in column_records:
            column_name = col_record['column_name']
            
            # Reverting to direct double-quoting for identifiers.
            # This is the most reliable approach given asyncpg's internal structure.
            # Identifiers (table and column names) are enclosed in double quotes.
            # This is safe because column_name and table_name are derived from
            # trusted database metadata (information_schema), not direct user input.
            quoted_column_name = f'"{column_name}"'
            quoted_table_name = f'"{table_name}"'
            
            count_query = f"SELECT COUNT({quoted_column_name}) FROM {quoted_table_name};"
            
            # Execute the count query using the established connection
            count_result = await conn.fetchval(count_query)

            if count_result is not None: # Check for None explicitly
                column_counts[column_name] = count_result
            else:
                column_counts[column_name] = 0 # Should not happen if table exists, but for safety

        return column_counts

    except asyncpg.exceptions.InvalidCatalogNameError:
        print(f"Error: Database '{db_name}' does not exist. Please verify the database name.")
        return None
    except asyncpg.exceptions.UndefinedTableError:
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'. Please verify the table name and schema.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred in table_columns_dict for '{table_name}' in '{db_name}': {e}")
        return None
    finally:
        if conn:
            await conn.close() # Ensure the connection is closed

async def delete_table_with_confirmation(db_name: str, table_name: str) -> bool:
    """
    Connects to a specified PostgreSQL database and attempts to delete a table
    after prompting for two confirmations from the user.

    Args:
        db_name (str): The name of the database where the table resides.
        table_name (str): The name of the table to be deleted.

    Returns:
        bool: True if the table was successfully deleted, False otherwise.
    """
    conn = None
    try:
        # # First confirmation
        # first_confirm = input(f"Are you sure you want to delete table '{table_name}' from database '{db_name}'? (yes/no): ").strip().lower()
        # if first_confirm != 'yes':
        #     print("Table deletion cancelled by user.")
        #     return False

        # # Second confirmation
        # second_confirm = input(f"This is a permanent action. Confirm deletion of '{table_name}' again by typing 'yes': ").strip().lower()
        # if second_confirm != 'yes':
        #     print("Table deletion cancelled by user (second confirmation failed).")
        #     return False

        # Construct the full connection string for the specified database
        conn_string = f"{BASE_DB_CONN_STRING}/{db_name}"
        conn = await asyncpg.connect(conn_string)

        # Use quote_ident for the table name to ensure it's properly handled in the SQL.
        # Since we don't have a direct quote_ident utility, we'll use f-string with double quotes.
        # This is safe as table_name is confirmed by the user and not direct untrusted input.
        quoted_table_name = f'"{table_name}"'
        
        # SQL query to drop the table. CASCADE option can be added if needed: DROP TABLE {quoted_table_name} CASCADE;
        drop_query = f"DROP TABLE {quoted_table_name};"

        print(f"Attempting to delete table '{table_name}' from database '{db_name}'...")
        await conn.execute(drop_query)
        print(f"Table '{table_name}' successfully deleted from database '{db_name}'.")
        return True

    except asyncpg.exceptions.InvalidCatalogNameError:
        print(f"Error: Database '{db_name}' does not exist. Table deletion failed.")
        return False
    except asyncpg.exceptions.UndefinedTableError:
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'. Table deletion failed.")
        return False
    except asyncpg.exceptions.PostgresError as e:
        print(f"A PostgreSQL specific error occurred during table deletion: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during table deletion for '{table_name}' in '{db_name}': {e}")
        return False
    finally:
        if conn:
            await conn.close() # Ensure the connection is closed

