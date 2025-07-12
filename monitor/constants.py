import os
from database.engine import init_db
from dotenv import load_dotenv
load_dotenv('this_is_not_an_env.env')

USER=os.getenv("USER")
PASSWORD=os.getenv("PASSWORD")
HOST=os.getenv("HOST") 
PORT=os.getenv("PORT")
DEFAULT_DATABASE_NAME=os.getenv("DEFAULT_DATABASE_NAME")

DEFAULT_CONN_STRING="postgresql+asyncpg://jon:3214@localhost:5432/chessism"