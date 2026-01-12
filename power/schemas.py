from enum import Enum
from typing import List, Optional
from ninja import Schema
from datetime import date
from pydantic import Field

class StateShortEnum(str, Enum):
    "Short code = value, full name = key"

    DL = "Delhi"
    MH = "Maharashtra"
    TN = "Tamil Nadu"
    UP = "Uttar Pradesh"
    AP = "Andhra Pradesh"
    AR = "Arunachal Pradesh"
    AS = "Assam"
    BR = "Bihar"
    CH = "Chandigarh"
    CG = "Chhattisgarh"
    GA = "Goa"
    GJ = "Gujarat"
    HR = "Haryana"
    HP = "Himachal Pradesh"
    JK = "J & K"
    JH = "Jharkhand"
    KA = "Karnataka"
    KL = "Kerala"
    MN = "Manipur"
    ML = "Meghalaya"
    MZ = "Mizoram"
    MP = "Madhya Pradesh"
    NL = "Nagaland"
    OD = "Odisha"
    PY = "Pondicherry"
    PB = "Punjab"
    RJ = "Rajasthan"
    SK = "Sikkim"
    TS = "Telangana"
    TR = "Tripura"
    UK = "Uttarakhand"
    WB = "West Bengal"



class DateQuerySchema(Schema):
    forecast_date: date = Field(
        default_factory=date.today,
        description="Date in YYYY-MM-DD format",
        example=date.today()
    )



class HourlyPointSchema(Schema):
    datetime: str
    mw: float
    temperature: float




class ForecastHourlyOut(Schema):
    state: str
    date: str
    season: str
    weekday: str
    is_weekend: bool
    is_holiday: bool
    energy_consumption_mu_per_day: float
    average_load_mw: float
    peak_load_mw: float
    mape_difference_percent: Optional[float] = None
    points: list[HourlyPointSchema]





class Forecast15MinPoint(Schema):
    datetime: str
    mw: float

class Forecast15MinOut(Schema):
    state: str
    date: str
    points: List[Forecast15MinPoint]





class TemperatureHourly(Schema):
    time: str
    temp: float

class TemperatureOut(Schema):
    state: str
    date: str
    average_temperature: float
    hourly: list[TemperatureHourly]




class PreviousPredictionItem(Schema):
    state: str
    date: str
    load_mw: float

class PreviousPredictionOut(Schema):
    count: int
    results: List[PreviousPredictionItem]


