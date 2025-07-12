# monitor.routers tables.py

import asyncio
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from monitor.operations.tables import *

router = APIRouter()

@router.get("/tables/column_dicts/{db_name}/{table_name}")
async def api_get_table_columns_dict(db_name,table_name):
    try:
        table_dict = await get_table_columns_dict(db_name,table_name) 
        return JSONResponse(content=table_dict)
    except Exception as e:
        print(f"Error: {e}")
        return JSONResponse(content={"error": f"Something:{e}"}, status_code=500)
@router.get("/tables/delete/{db_name}/{table_name}")
async def api_get_delete_table(db_name,table_name):
    try:
        await get_delete_table(db_name,table_name) 
        return JSONResponse(content={f"{table_name}":"deleted"})
    except Exception as e:
        print(f"Error: {e}")
        return JSONResponse(content={"error": f"Something:{e}"}, status_code=500)
