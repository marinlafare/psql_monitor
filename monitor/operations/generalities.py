# monitor/operations/generalities.py

from monitor.database.ask_db_generalities import *

async def get_general_dict():
    return await get_db_connection_strings_and_tables_dict()
async def get_general_size():
    return await get_dbs_general_size()
async def get_db_size(db_name):
    return await get_one_db_size(db_name)
async def get_one_table_size(db_name, table_name):
    return await get_table_size(db_name, table_name)