# monitor.routers generalities.py

import asyncio
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from monitor.operations.generalities import *

router = APIRouter()

@router.get("/general/general_dict")
async def api_get_general_dict():
    try:
        general_dict = await get_general_dict() 
        return JSONResponse(content=general_dict)
    except Exception as e:
        print(f"Error: {e}")
        return JSONResponse(content={"error": f"Something:{e}"}, status_code=500)
@router.get("/general/general_size")
async def api_get_general_size():
    try:
        general_size = await get_general_size()
        return JSONResponse(content=general_size)
    except Exception as e:
        print(f"Error: {e}")
        return JSONResponse(content={"error": f"Something: {e}"}, status_code=500)
@router.get("/general/{db_name}/size")
async def api_get_db_size(db_name):
    try:
        one_db_size = await get_db_size(db_name) 
        return JSONResponse(content=one_db_size)
    except Exception as e:
        print(f"Error: {e}")
        return JSONResponse(content={"error": f"Something: {e}"}, status_code=500)
@router.get("/general/{db_name}/{table_name}/size")
async def api_get_one_table_size(db_name, table_name):
    try:
        one_table_size = await get_one_table_size(db_name, table_name) 
        return JSONResponse(content=one_table_size)
    except Exception as e:
        print(f"Error: {e}")
        return JSONResponse(content={"error": f"Something: {e}"}, status_code=500)