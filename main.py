# MONITOR MAIN (main.py)
import os
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
from contextlib import asynccontextmanager
from constants import DEFAULT_CONN_STRING
from monitor.database.engine import init_db

from monitor.database.routers import databases_dict


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events for the FastAPI application.
    Initializes the asynchronous database connection.
    """
    print('Initializing MONITOR Server...')
    try:
        await init_db(DEFAULT_CONN_STRING)
        print('...MONITOR Server ON...')
        yield
    except Exception as e:
        print(f"Failed to start LEELA Server due to database initialization error: {e}")
        raise # Re-raise to prevent server from starting if DB init fails

    print('...MONITOR Server DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root(): # Changed to async def as good practice for FastAPI endpoints
    return "MONITOR server running."

# Include your existing routers
app.include_router(databases_dict.router)
