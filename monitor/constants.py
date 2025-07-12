# monitor/constants.py
import os
from dotenv import load_dotenv
from pathlib import Path

current_script_dir = Path(__file__).resolve().parent
env_file_name = '.env'
env_file_absolute_path = current_script_dir / env_file_name
load_dotenv(env_file_absolute_path, override=True)

# Retrieve environment variables.
# Explicitly set HOST and PORT here to force connection to host's PostgreSQL
# This is a debugging measure to bypass environment variable conflicts.
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

# Force HOST and PORT to the correct values for connecting to the host's PostgreSQL
HOST = "host.docker.internal" # For Docker Desktop (Windows/macOS)
# For Linux, if host.docker.internal doesn't work, you might need to use your host's IP
# HOST = "172.17.0.1" # Example for Linux if Docker bridge IP is 172.17.0.1
PORT = "5432" # Standard PostgreSQL port on the host

DEFAULT_DATABASE_NAME = os.getenv("DEFAULT_DATABASE_NAME")

# Convert PORT to int explicitly
if PORT is None:
    raise ValueError("PORT environment variable is not set. It must be provided by .env or Docker Compose.")
try:
    PORT_INT = int(PORT)
except ValueError:
    raise ValueError(f"PORT environment variable '{PORT}' cannot be converted to an integer.")


# --- Debugging Start (to confirm loaded values) ---
print(f"DEBUG (constants.py): Loaded USER: '{USER}'")
print(f"DEBUG (constants.py): Loaded PASSWORD: '{'*' * len(PASSWORD)}'") # Mask password
print(f"DEBUG (constants.py): Loaded HOST: '{HOST}'")
print(f"DEBUG (constants.py): Loaded PORT: '{PORT}' (as string)")
print(f"DEBUG (constants.py): Parsed PORT_INT: '{PORT_INT}'")
print(f"DEBUG (constants.py): Loaded DEFAULT_DATABASE_NAME: '{DEFAULT_DATABASE_NAME}'")
# --- Debugging End ---


# Construct connection strings using the retrieved (and now guaranteed non-None) values
DEFAULT_CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT_INT}/{DEFAULT_DATABASE_NAME}"
BASE_DB_CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT_INT}"


import asyncpg

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
        actual_conn_string = db_str
        
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




# # monitor/constants.py
# # import os
# # from dotenv import load_dotenv
# # from pathlib import Path
# # import asyncpg
# # current_script_dir = Path(__file__).resolve().parent
# # env_file_name = 'this_is_not_an_env.env'
# # env_file_absolute_path = current_script_dir / env_file_name
# # load_dotenv(env_file_absolute_path, override=True)

# # USER = os.getenv("USER")
# # PASSWORD = os.getenv("PASSWORD")
# # HOST = os.getenv('HOST')
# # PORT = os.getenv("PORT")
# # DEFAULT_DATABASE_NAME = os.getenv("DEFAULT_DATABASE_NAME")
# # DEFAULT_CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DEFAULT_DATABASE_NAME}"
# # BASE_DB_CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}"


# # monitor/constants.py
# import os
# from dotenv import load_dotenv
# from pathlib import Path

# # Get the absolute path to the directory where this script resides
# current_script_dir = Path(__file__).resolve().parent
# env_file_name = '.env'
# env_file_absolute_path = current_script_dir / env_file_name

# # Load environment variables from the .env file
# # This will load them into os.environ
# load_dotenv(env_file_absolute_path, override=True)

# # Retrieve environment variables with default fallbacks
# # This ensures that if a variable is not found in the .env file or environment,
# # it will use a default value instead of None, preventing errors.
# USER = os.getenv("USER") # Default to "jon" if not found
# PASSWORD = os.getenv("PASSWORD") # Default to "3214" if not found
# HOST = os.getenv("HOST") # Default to "localhost" if not found
# PORT = os.getenv("PORT") # Default to "5432" (as a string) if not found
# DEFAULT_DATABASE_NAME = os.getenv("DEFAULT_DATABASE_NAME") # Default to "chessism" if not found

# # --- Debugging Start (to confirm loaded values) ---
# print(f"DEBUG (constants.py): Loaded USER: '{USER}'")
# print(f"DEBUG (constants.py): Loaded PASSWORD: '{'*' * len(PASSWORD)}'") # Mask password
# print(f"DEBUG (constants.py): Loaded HOST: '{HOST}'")
# print(f"DEBUG (constants.py): Loaded PORT: '{PORT}'")
# print(f"DEBUG (constants.py): Loaded DEFAULT_DATABASE_NAME: '{DEFAULT_DATABASE_NAME}'")
# # --- Debugging End ---


# # Construct connection strings using the retrieved (and now guaranteed non-None) values
# DEFAULT_CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DEFAULT_DATABASE_NAME}"
# BASE_DB_CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}"


# import asyncpg
# # Removed: from asyncpg.utils import quote_ident # This import caused the ImportError

# # Import open_async_request from this same constants file
# # (Assuming open_async_request is defined here or will be moved here)
# # If open_async_request is in a separate file, ensure it imports BASE_DB_CONN_STRING
# # from this constants.py
# # from .database.engine import init_db # This import might not be needed directly in constants.py


# async def open_async_request(db_str: str,
#                              sql_question: str,
#                              params: tuple = None,
#                              fetch_as_dict: bool = False):
#     """
#     Executes an asynchronous SQL query, optionally with parameters, and fetches results.
#     Establishes and closes an asyncpg connection internally for each request.

#     Args:
#         db_str: The full PostgreSQL connection string for the target database.
#         sql_question: The SQL query string.
#         params: A tuple of parameters to pass to the query (asyncpg uses positional parameters like $1, $2).
#         fetch_as_dict: If True, fetches results as a list of dictionaries.
#                        Otherwise, returns a list of asyncpg.Record objects (which behave like tuples).

#     Returns:
#         A list of query results (dictionaries or asyncpg.Record objects).
#     """
#     conn = None
#     try:
#         # Replace 'postgresql' with 'postgresql' for direct asyncpg.connect.
#         actual_conn_string = db_str
        
#         conn = await asyncpg.connect(actual_conn_string)

#         if params:
#             rows = await conn.fetch(sql_question, *params)
#         else:
#             rows = await conn.fetch(sql_question)
        
#         if fetch_as_dict:
#             return [dict(row) for row in rows]
#         else:
#             return rows
#     except Exception as e:
#         print(f"Error in open_async_request: {e}")
#         raise # Re-raise to propagate the error
#     finally:
#         if conn:
#             await conn.close()




# async def open_async_request(db_str: str,
#                              sql_question: str,
#                              params: tuple = None,
#                              fetch_as_dict: bool = False):
#     """
#     Executes an asynchronous SQL query, optionally with parameters, and fetches results.
#     Establishes and closes an asyncpg connection internally for each request.

#     Args:
#         db_str: The full PostgreSQL connection string for the target database.
#         sql_question: The SQL query string.
#         params: A tuple of parameters to pass to the query (asyncpg uses positional parameters like $1, $2).
#         fetch_as_dict: If True, fetches results as a list of dictionaries.
#                        Otherwise, returns a list of asyncpg.Record objects (which behave like tuples).

#     Returns:
#         A list of query results (dictionaries or asyncpg.Record objects).
#     """
#     conn = None
#     try:
#         # Replace 'postgresql' with 'postgresql' for direct asyncpg.connect.
#         actual_conn_string = db_str
        
#         conn = await asyncpg.connect(actual_conn_string)

#         if params:
#             rows = await conn.fetch(sql_question, *params)
#         else:
#             rows = await conn.fetch(sql_question)
        
#         if fetch_as_dict:
#             return [dict(row) for row in rows]
#         else:
#             return rows
#     except Exception as e:
#         print(f"Error in open_async_request: {e}")
#         raise # Re-raise to propagate the error
#     finally:
#         if conn:
#             await conn.close()