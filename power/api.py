from datetime import date, datetime
from threading import Thread
from ninja import Query, Router, File
from ninja.files import UploadedFile
import pandas as pd
from power.ml.disaggregate import disaggregate_hourly_to_15min
from power.ml.manage_models import train_all_models
from power.ml.weather import fetch_and_save_weather
from power.models import DailyPredictionHistory, WeatherHourly
from power.schemas import DateQuerySchema, Forecast15MinOut, ForecastHourlyOut, LiveMAPEOut, PreviousPredictionItem, PreviousPredictionOut, StateOut, StateShortEnum, TemperatureOut
from power.utils.upload import save_power_data_from_xlsx
from ninja.errors import HttpError
from django.db.models import Avg
from power.utils.forecast import get_forecast_5min_data, get_hourly_forecast_data
from power.utils.metadata import STATE_IN , STATE_CODE_TO_NAME
from typing import List
from ninja.pagination import paginate, PageNumberPagination


router = Router()


@router.post("/upload-xlsx")
def upload_xlsx(request, file: UploadedFile = File(...)):

    """
    **URL:** POST /upload-xlsx  
    **Description:** Upload XLSX file containing historical electricity data. Automatically saves to DB and retrains ML models.  

    **Payload:**
    - file: XLSX file

    **Response 200 OK:**
    ```json
    {
        "status": "success",
        "rows_inserted": 123,
        "ml_status": "retrained"
    }
    ```

    **Error 400:** Invalid file or missing columns
    """

    try:
        rows_inserted = save_power_data_from_xlsx(file)
    except ValueError as e:
        raise HttpError(status_code=400, message=f"{e}")

    # # Auto-train ML model
    # train_model()

    return {
        "status": "success",
        "rows_inserted": rows_inserted,
        "ml_status": "retrained"
    }





@router.post("/train-all-models/", response={200: dict})
def train_all_models_api(request):
    """
    **URL:** POST /train-all-models/  
    **Description:** Trains all ML models for all states and regions in background.  

    **Response 200 OK:**
    ```json
    {
        "message": "Model training has started in the background. Check logs for progress."
    }
    ```
    """

    def background_training():
        try:
            train_all_models()
        except Exception as e:
            print("Error during model training:", str(e))
            

    # Run training in background thread
    Thread(target=background_training).start()

    return {"message": "Model training has started in the background. Check logs for progress."}



@router.get("/states/in", response=List[StateOut])
def list_states_in(request):
    """
    **URL:** GET /states/in  
    **Description:** Returns list of all Indian states with short code and full name.  

    **Response 200 OK:**
    ```json
    [
        {"code": "DL", "name": "Delhi"},
        {"code": "MH", "name": "Maharashtra"}
    ]
    ```
    """

    return STATE_IN




@router.get("/forecast-hourly", response=ForecastHourlyOut)
def forecast_hourly(request, state_code: StateShortEnum, query: DateQuerySchema = Query(...)):

    """
    **URL:** GET /forecast-hourly  
    **Description:** Returns hourly forecast for a given state and date.  

    **Query Params:**
    - state_code: Short code of the state (Dropdown)
    -- example: DL, MH, TN, UP, AP, AR, AS, BR, CH, CG, GA, GJ, HR, HP, JK, JH, KA, KL, MN, ML, MZ, MP, NL, OD, PY, PB, RJ, SK, TS, TR, UK, WB
    - forecast_date: YYYY-MM-DD (optional, defaults to today)

    **Response 200 OK Example:**
    ```json
    {
        "state": "West Bengal",
        "date": "2026-01-09",
        "season": "Winter",
        "weekday": "Monday",
        "is_weekend": false,
        "is_holiday": false,
        "energy_consumption_mu_per_day": 1234.56,
        "average_load_mw": 500.25,
        "peak_load_mw": 750.40,
        "mape_difference_percent": 3.5,
        "points": [
            {"datetime": "2026-01-09T00:00:00", "mw": 480.5, "temperature": 18.2}
        ]
    }
    ```
    """

    data = get_hourly_forecast_data(state_code, query.forecast_date)
    data["state"] = STATE_CODE_TO_NAME.get(state_code.value, state_code.value)
    return data





@router.get("/forecast-15min", response=Forecast15MinOut)
def forecast_15min(
    request,
    state_code: StateShortEnum,
    query: DateQuerySchema = Query(...)
):
    """
    **URL:** GET /forecast-15min  
    **Description:** Returns 15-minute forecast by disaggregating hourly forecast.

    - state_code: Short code of the state (Dropdown)
    - forecast_date: YYYY-MM-DD (optional, defaults to today)
    """

    # =====================================================
    # 1️⃣ Get forecast date
    # =====================================================
    forecast_date = query.forecast_date

    # =====================================================
    # 2️⃣ Get hourly forecast (already has daily_temperature)
    # =====================================================
    hourly = get_hourly_forecast_data(state_code, forecast_date)

    # =====================================================
    # 3️⃣ Convert hourly points → DataFrame
    # =====================================================
    df = pd.DataFrame(hourly["points"])
    df["ds"] = pd.to_datetime(df["datetime"])
    df["yhat"] = df["mw"]

    # =====================================================
    # 4️⃣ Disaggregate hourly → 15-minute
    # =====================================================
    df_15 = disaggregate_hourly_to_15min(df)

    # =====================================================
    # 5️⃣ Build response points
    # =====================================================
    points = [
        {
            "datetime": row.ds.isoformat(),
            "mw": round(row.yhat, 2)
        }
        for row in df_15.itertuples()
    ]

    # =====================================================
    # 6️⃣ Final response (WITH daily temperature)
    # =====================================================
    return {
    "state": STATE_CODE_TO_NAME.get(state_code.value, hourly["state"]),
    "date": forecast_date.isoformat(),
    "daily_temperature": hourly.get("daily_temperature"),
    "points": points
}






@router.get("/temperature", response=TemperatureOut)
def temperature_api(request, state_code: StateShortEnum, query: DateQuerySchema = Query(...)):

    """
    **URL:** GET /temperature  
    **Description:** Returns hourly temperature and daily average temperature for a state. Fetches data automatically if missing.  

    **Query Params:**
    - state_code: Short code of the state (Dropdown)
    -- example: DL, MH, TN, UP, AP, AR, AS, BR, CH, CG, GA, GJ, HR, HP, JK, JH, KA, KL, MN, ML, MZ, MP, NL, OD, PY, PB, RJ, SK, TS, TR, UK, WB
    - forecast_date: YYYY-MM-DD (optional, defaults to today)

    **Response 200 OK Example:**
    ```json
    {
        "state": "WB",
        "date": "2026-01-09",
        "average_temperature": 22.5,
        "hourly": [
            {"time": "2026-01-09T00:00:00", "temp": 21.5}
        ]
    }
    ```
    """

    forecast_date = query.forecast_date

    qs = WeatherHourly.objects.filter(
        state=state_code.value,
        datetime__date=forecast_date
    )

    # 🔥 AUTO FETCH IF DATA NOT FOUND
    if not qs.exists():
        fetch_and_save_weather(
            state_short=state_code.value,
            start_date=forecast_date,
            frequency="hourly"
        )

        qs = WeatherHourly.objects.filter(
            state=state_code.value,
            datetime__date=forecast_date
        )

    # STILL EMPTY → graceful response
    if not qs.exists():
        return {
            "state": state_code.value,
            "date": forecast_date.isoformat(),
            "average_temperature": None,
            "hourly": []
        }

    avg_temp = qs.aggregate(t=Avg("temperature_c"))["t"]

    hourly = [
        {
            "time": obj.datetime.isoformat(),
            "temp": round(obj.temperature_c, 1)
        }
        for obj in qs.order_by("datetime")
    ]

    return {
        "state": STATE_CODE_TO_NAME.get(state_code.value,state_code.value),
        "date": forecast_date.isoformat(),
        "average_temperature": round(avg_temp, 1),
        "hourly": hourly
    }





@router.get(
    "/previous-predictions",
    response=List[PreviousPredictionItem]
)
@paginate(PageNumberPagination, page_size=10)   # 👈 DIRECT PAGE SIZE
def previous_predictions(
    request,
    state_code: StateShortEnum,
    date:DateQuerySchema = Query(...)
): 
    """
    **URL:** GET /previous-predictions  
    **Description:** Returns previously saved daily predictions. Supports pagination.  

    **Query Params:**
    - state_code: Short code of the state (Dropdown)
    -- example: DL, MH, TN, UP, AP, AR, AS, BR, CH, CG, GA, GJ, HR, HP, JK, JH, KA, KL, MN, ML, MZ, MP, NL, OD, PY, PB, RJ, SK, TS, TR, UK, WB
    - forecast_date: YYYY-MM-DD (optional, defaults to today)

    **Response 200 OK Example:**
    ```json
    [
        {"state": "WB", "date": "2026-01-09", "load_mw": 480.5},
        {"state": "WB", "date": "2026-01-08", "load_mw": 470.3}
    ]
    ```
    """


    qs = DailyPredictionHistory.objects.all()

    if state_code:
        qs = qs.filter(state=state_code.value)

    if date:
        qs = qs.filter(date=date.forecast_date)

    return [
        {
            "state": STATE_CODE_TO_NAME.get(state_code.value,obj.state),
            "date": obj.date.isoformat(),
            "load_mw": round(obj.load_mw, 2)
        }
        for obj in qs
    ]






@router.post("/live-mape", response=LiveMAPEOut)
def live_mape_api(
    request,
    state_code: StateShortEnum,
    actual_load_mw: float,
    from_time: str,
    to_time: str,
):
    
    """
    **URL:** POST /live-mape  
    **Description:**  
    Calculates live MAPE (Mean Absolute Percentage Error) by comparing  
    actual load with predicted hourly forecast load for a given time range  
    of the current day.

    This API is useful for **real-time forecast accuracy monitoring**.

    **Query Params:**
    - state_code: Short code of the state (Dropdown)  
    -- example: DL, MH, TN, UP, AP, AR, AS, BR, CH, CG, GA, GJ, HR, HP, JK,  
        JH, KA, KL, MN, ML, MZ, MP, NL, OD, PY, PB, RJ, SK, TS, TR, UK, WB
    - actual_load_mw: Actual measured load in MW  
    -- example: 4520.75
    - from_time: Start time in HH:MM (24-hour format)  
    -- example: 10:00
    - to_time: End time in HH:MM (24-hour format)  
    -- example: 14:00

    **Notes:**
    - forecast_date is automatically set to **today**
    - from_time must be earlier than to_time
    - MAPE is calculated using average predicted load within the selected time range

    **Response 200 OK Example:**
    ```json
    {
    "state": "DL",
    "date": "2026-01-12",
    "from_time": "10:00",
    "to_time": "14:00",
    "actual_load_mw": 4520.75,
    "predicted_load_mw": 4389.62,
    "mape_percent": 2.9,
    "status": "EXCELLENT"
    }
    ``` 
    """

    # -------------------------------------------------
    # DATE HANDLING (DEFAULT = TODAY)
    # -------------------------------------------------
    forecast_date = date.today()
    

    # -------------------------------------------------
    # TIME PARSING
    # -------------------------------------------------
    try:
        from_t = datetime.strptime(from_time, "%H:%M").time()
        to_t = datetime.strptime(to_time, "%H:%M").time()
    except ValueError:
        raise HttpError(400, "Time format must be HH:MM")

    if from_t >= to_t:
        raise HttpError(400, "from_time must be before to_time")

    # -------------------------------------------------
    # FORECAST DATA
    # -------------------------------------------------
    forecast = get_hourly_forecast_data(
        state_short=state_code,
        date=forecast_date.isoformat()
    )

    hourly_points = forecast["points"]

    # -------------------------------------------------
    # FILTER TIME RANGE
    # -------------------------------------------------
    selected = [
        p["mw"]
        for p in hourly_points
        if from_t <= datetime.fromisoformat(p["datetime"]).time() < to_t
    ]

    if not selected:
        raise HttpError(404, "No forecast data in selected time range")

    predicted_load_mw = round(sum(selected) / len(selected), 2)

    # -------------------------------------------------
    # MAPE
    # -------------------------------------------------
    mape_percent = round(
        abs(actual_load_mw - predicted_load_mw) / actual_load_mw * 100,
        2
    )

    # -------------------------------------------------
    # STATUS
    # -------------------------------------------------
    if mape_percent <= 5:
        status = "EXCELLENT"
    elif mape_percent <= 10:
        status = "GOOD"
    elif mape_percent <= 20:
        status = "ACCEPTABLE"
    else:
        status = "POOR"

    return {
        "state": STATE_CODE_TO_NAME.get(state_code.value,state_code.value),
        "date": forecast_date.isoformat(),
        "from_time": from_time,
        "to_time": to_time,
        "actual_load_mw": actual_load_mw,
        "predicted_load_mw": predicted_load_mw,
        "mape_percent": mape_percent,
        "status": status
    }




@router.get("/forecast-5min", response=ForecastHourlyOut)
def forecast_5min(
    request,
    state_code: StateShortEnum,
    query: DateQuerySchema = Query(...)
):
    """
    **URL:** GET /forecast-5min  
    **Description:** Returns 5-minute forecast for a given state and date.  

    **Query Params:**
    - state_code: Short code of the state (Dropdown)
      -- example: DL, MH, TN, UP, AP, AR, AS, BR, CH, CG, GA, GJ, HR, HP, JK, JH, KA, KL, MN, ML, MZ, MP, NL, OD, PY, PB, RJ, SK, TS, TR, UK, WB
    - forecast_date: YYYY-MM-DD (optional, defaults to today)

    **Response 200 OK Example:**
    ```json
    {
        "state": "Delhi",
        "date": "2026-01-15",
        "average_load_mw": 3968.77,
        "peak_load_mw": 4696.96,
        "points": [
            {"datetime": "2026-01-15T00:00:00", "mw": 3530.95},
            {"datetime": "2026-01-15T00:05:00", "mw": 3540.12}
        ]
    }
    ```
    """
    data = get_forecast_5min_data(state=state_code, forecast_date=query.forecast_date)
    data["state"] = STATE_CODE_TO_NAME.get(state_code.value, state_code.value)
    return data
