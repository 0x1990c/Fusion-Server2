import requests

from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from database import AsyncSessionLocal
from app.Utils.sendgrid import alert_courts_admin
from app.Model.CaseModel import TimeRange
from app.Model.CaseModel import FilterCondition
from app.Model.CaseModel import AlertAdminData
import app.Utils.database_handler as crud
import app.Utils.Auth as Auth
from typing import Annotated
from dotenv import load_dotenv

from bs4 import BeautifulSoup

load_dotenv()
router = APIRouter()

# Dependency to get the database session
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


@router.post("/getCases")
async def getCases(timeRange: TimeRange, db: Session = Depends(get_db)):
    try:
        cases = await crud.get_cases(db, timeRange)
        return {"cases": cases}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/getCounties")
async def getCounties(timeRange: TimeRange, db: Session = Depends(get_db)):
    try:
        counties = await crud.get_counties(db, timeRange)
        return {"counties": counties}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/getData")
async def getData(filterCondition: FilterCondition, db: Session = Depends(get_db)):
    try:
        cases = await crud.get_data(db, filterCondition)
        return {"cases": cases}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/getDataForMerge")
async def getDataForMerge(filterCondition: FilterCondition, db: Session = Depends(get_db)):
    try:
        print("getDataForMerge:")
        cases = await crud.get_data_merge(db, filterCondition)
        return {"cases": cases}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/getLastQueryDate")
async def getLastQueryDate(db: Session = Depends(get_db)):
    try:
        queryDate = await crud.get_last_query_date(db)
        return {"query_date": queryDate}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fetchCourts")
async def fetchCourts(db: Session = Depends(get_db)):
    try:
        url = "https://www.in.gov/courts/help/odyssey-courts/"

        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            tables = soup.find_all('table')

            if len(tables) >= 2:
                second_table = tables[1]  # Get the second table
                data = []
                for row in second_table.find_all('tr'):
                    cols = row.find_all('td')
                    if cols:
                        data.append([col.get_text(strip=True) for col in cols])       
                resultFlag = await crud.insert_courts(db, data)
                return {"success": resultFlag}
            else:
                print("The second table was not found on the page.")
                return {"success": False}
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return {"success": False}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/fetchCounties")
async def fetchCounties(db: Session = Depends(get_db)):
    try:
        url = "https://www.in.gov/courts/local/"

        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            section = soup.find("section", id="645676")

            counties = []
            for a in section.find_all("a", href=True):
                county_name = a.text.strip()
                counties.append({"name": county_name})
                
            resultFlag = await crud.insert_counties(db, counties)
            return {"success": resultFlag}
        
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return {"success": False}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/getCourts")
async def getCourts(db: Session = Depends(get_db)):
    try:
        courts = await crud.get_courts(db)
        return {"courts": courts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.post("/alertCourtsToAdmin")
async def alertCourtsToAdmin(alertAdminData: AlertAdminData, db: Session = Depends(get_db)):
    try:
        await alert_courts_admin(alertAdminData, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))