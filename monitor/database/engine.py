import os
from urllib.parse import urlparse
import asyncpg
from asyncpg import exceptions

async def init_db(connection_string: str):
    """
    Establishes an asyncpg connection to the specified PostgreSQL database.
    It will attempt to create the database if it does not exist.

    Args:
        connection_string: The full PostgreSQL connection string
                           (e.g., "postgresql+asyncpg://user:pass@host:port/dbname").

    Returns:
        An asyncpg.Connection object if successful, otherwise raises an exception.
    """
    # Parse the connection string to extract components
    parsed_url = urlparse(connection_string)

    db_user = parsed_url.username
    db_password = parsed_url.password
    db_host = parsed_url.hostname
    db_port = parsed_url.port if parsed_url.port else 5432 # Default PostgreSQL port

    # Ensure db_name is a string, even if parsed_url.path is bytes
    db_name_raw = parsed_url.path
    if isinstance(db_name_raw, bytes):
        db_name = db_name_raw.decode('utf-8').lstrip('/')
    else:
        db_name = db_name_raw.lstrip('/')

    temp_conn = None
    try:
        # Construct a connection string without the specific database name for admin connection.
        # This allows us to connect to a default database (like 'postgres') to perform
        # administrative tasks such as checking if a database exists or creating one.
        # Ensure the admin connection string is correctly formed, handling potential None for user/password
        admin_user_pass = ""
        if db_user:
            admin_user_pass = f"{db_user}"
            if db_password:
                admin_user_pass += f":{db_password}"
            admin_user_pass += "@"

        # FIX: Change the scheme for direct asyncpg connections to 'postgresql' or 'postgres'
        # asyncpg does not understand 'postgresql+asyncpg' directly.
        admin_conn_string = f"postgresql://{admin_user_pass}{db_host}:{db_port}/postgres"
        
        print(f"Connecting to admin database for checks: {admin_conn_string}")
        temp_conn = await asyncpg.connect(admin_conn_string)
        
        # Check if the target database exists
        db_exists = await temp_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )

        if not db_exists:
            print(f"Database '{db_name}' does not exist. Attempting to create it...")
            # Close the admin connection before creating the database, as you can't create
            # a database while connected to it directly.
            await temp_conn.close()
            temp_conn = None # Set to None to ensure finally block doesn't try to close it again

            # Re-connect to a default database to execute the CREATE DATABASE command
            # Re-establish admin connection
            temp_conn = await asyncpg.connect(admin_conn_string)
            await temp_conn.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")

    except exceptions.DuplicateDatabaseError:
        print(f"Database '{db_name}' already exists (caught DuplicateDatabaseError).")
    except Exception as e:
        print(f"Error during database check/creation: {e}")
        # Re-raise the exception if the database couldn't be prepared
        raise
    finally:
        if temp_conn:
            await temp_conn.close()

    # Now, establish the actual connection to the target database for your application logic
    # The original connection_string (with 'postgresql+asyncpg') is used here,
    # assuming that if init_db is called with it, it's intended for a context
    # that handles this scheme (e.g., SQLAlchemy's engine creation).
    # If this function is *only* used for direct asyncpg connections,
    # you might want to consider removing the '+asyncpg' from the input
    # connection_string as well, or handling it here.
    # For now, we'll assume the final connection also expects 'postgresql' or 'postgres'
    # if asyncpg.connect is called directly.
    
    # FIX: Ensure the final connection also uses 'postgresql' or 'postgres' scheme
    # if it's a direct asyncpg.connect call.
    final_conn_string = connection_string.replace("postgresql+asyncpg://", "postgresql://")

    print(f"Establishing final connection to: {final_conn_string}")
    try:
        conn = await asyncpg.connect(final_conn_string)
        print(f"Successfully connected to {db_name}.")
        return conn
    except Exception as e:
        print(f"Failed to establish final connection to {db_name}: {e}")
        raise

