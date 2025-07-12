# monitor/operations/tables.py

from monitor.database.ask_db_tables import *

async def get_table_columns_dict(db_name, table_name):
    return await table_columns_dict(db_name, table_name)
async def get_delete_table(db_name, table_name):
    return await delete_table_with_confirmation(db_name, table_name)
