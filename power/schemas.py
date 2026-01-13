from enum import Enum
from typing import List, Optional
from ninja import Schema
from datetime import date
from pydantic import Field

class StateShortEnum(str, Enum):
    DL = "DL"
    MH = "MH"
    TN = "TN"
    UP = "UP"
    AP = "AP"
    AR = "AR"
    AS = "AS"
    BR = "BR"
    CH = "CH"
    CG = "CG"
    GA = "GA"
    GJ = "GJ"
    HR = "HR"
    HP = "HP"
    JK = "JK"
    JH = "JH"
    KA = "KA"
    KL = "KL"
    MN = "MN"
    ML = "ML"
    MZ = "MZ"
    MP = "MP"
    NL = "NL"
    OD = "OD"
    PY = "PY"
    PB = "PB"
    RJ = "RJ"
    SK = "SK"
    TS = "TS"
    TR = "TR"
    UK = "UK"
    WB = "WB"





class StateOut(Schema):
    code: str
    name: str



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



class LiveMAPEOut(Schema):
    state: str
    date: str
    from_time: str
    to_time: str
    actual_load_mw: float
    predicted_load_mw: float
    mape_percent: float
    status: str