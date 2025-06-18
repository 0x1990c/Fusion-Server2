from pydantic import BaseModel
from datetime import datetime
from typing import List

class TimeRange(BaseModel):
    fromDate: datetime
    toDate: datetime

class FilterCondition(BaseModel):
    fromDate: datetime
    toDate: datetime
    offset: int
    selectedCaseTypes: List[str]
    selectedCourt: List[str]
    selectedCounty: List[str]
    username: str
    
class AlertAdminData(BaseModel):
    county: str
    court: str
    user: str

class UserNameModel(BaseModel):
    username: str

class ShortcodeModel(BaseModel):
    field: str
    shortcode: str
