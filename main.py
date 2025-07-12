# monitor/main.py

import os
from fastapi import FastAPI, WebSocket
from contextlib import asynccontextmanager
from monitor.constants import DEFAULT_CONN_STRING
from monitor.database.engine import init_db

from monitor.routers import generalities, tables


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
        print(f"Failed to start MONITOR initialization error: {e}")
        raise # Re-raise to prevent server from starting if DB init fails

    print('...MONITOR Server DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return "MONITOR server running."

# routers
app.include_router(generalities.router)
app.include_router(tables.router)

