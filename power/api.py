from threading import Thread
from ninja import Query, Router, File
from ninja.files import UploadedFile
import pandas as pd
from power.ml.disaggregate import disaggregate_hourly_to_15min
from power.ml.manage_models import train_all_models
from power.ml.weather import fetch_and_save_weather
from power.models import DailyPredictionHistory, WeatherHourly
from power.schemas import DateQuerySchema, Forecast15MinOut, ForecastHourlyOut, PreviousPredictionItem, PreviousPredictionOut, StateShortEnum, TemperatureOut
from power.utils.upload import save_power_data_from_xlsx
from ninja.errors import HttpError
from django.db.models import Avg
from power.utils.forecast import get_hourly_forecast_data
from typing import List
from ninja.pagination import paginate, PageNumberPagination





router = Router()




@router.post("/upload-xlsx")
def upload_xlsx(request, file: UploadedFile = File(...)):
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
    Endpoint to train all region and state models.
    Runs in a separate thread to prevent request timeout.
    """

    def background_training():
        try:
            train_all_models()
        except Exception as e:
            print("Error during model training:", str(e))
            

    # Run training in background thread
    Thread(target=background_training).start()

    return {"message": "Model training has started in the background. Check logs for progress."}




@router.get("/forecast-hourly/{state_short}", response=ForecastHourlyOut)
def forecast_hourly(request, state_short: StateShortEnum, query: DateQuerySchema = Query(...)):
    return get_hourly_forecast_data(state_short, query.forecast_date)





@router.get("/forecast-15min/{state}", response=Forecast15MinOut)
def forecast_15min(
    request,
    state: StateShortEnum,
    query: DateQuerySchema = Query(...)
):
    forecast_date = query.forecast_date

    hourly = get_hourly_forecast_data(state, forecast_date)

    df = pd.DataFrame(hourly["points"])
    df["ds"] = pd.to_datetime(df["datetime"])
    df["yhat"] = df["mw"]

    df_15 = disaggregate_hourly_to_15min(df)

    points = [
        {
            "datetime": row.ds.isoformat(),
            "mw": round(row.yhat, 2)
        }
        for row in df_15.itertuples()
    ]

    return {
        "state": hourly["state"],
        "date": forecast_date.isoformat(),
        "points": points
    }





@router.get("/temperature/{state}", response=TemperatureOut)
def temperature_api(request, state: StateShortEnum, query: DateQuerySchema = Query(...)):
    forecast_date = query.forecast_date

    qs = WeatherHourly.objects.filter(
        state=state.name,
        datetime__date=forecast_date
    )

    # 🔥 AUTO FETCH IF DATA NOT FOUND
    if not qs.exists():
        fetch_and_save_weather(
            state_short=state.name,
            start_date=forecast_date,
            frequency="hourly"
        )

        qs = WeatherHourly.objects.filter(
            state=state.name,
            datetime__date=forecast_date
        )

    # STILL EMPTY → graceful response
    if not qs.exists():
        return {
            "state": state.name,
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
        "state": state.name,
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
    state: StateShortEnum,
    date:DateQuerySchema = Query(...)
): 

    qs = DailyPredictionHistory.objects.all()

    if state:
        qs = qs.filter(state=state.name)

    if date:
        qs = qs.filter(date=date.forecast_date)

    return [
        {
            "state": obj.state,
            "date": obj.date.isoformat(),
            "load_mw": round(obj.load_mw, 2)
        }
        for obj in qs
    ]

