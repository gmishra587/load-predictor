from datetime import datetime
import pandas as pd
from ninja.errors import HttpError
from django.db import transaction

from power.schemas import StateShortEnum
from power.utils.backgroundwork import background_work
from power.utils.metadata import add_calendar_features, day_metadata
from power.utils.validation import validate_date
from power.ml.disaggregate import disaggregate
from power.models import DailyPredictionHistory


def get_hourly_forecast_data(state_short: StateShortEnum, date: str):
    # =====================================================
    # 1️⃣ Validate date
    # =====================================================
    forecast_date = validate_date(date)

    # =====================================================
    # 2️⃣ Load weather + models
    # =====================================================
    weather_data, region_model, state_model = background_work(
        state_short=state_short.name,
        start_date=forecast_date,
        frequency="hourly"
    )

    # =====================================================
    # 3️⃣ Create future hourly dataframe
    # =====================================================
    future = pd.DataFrame({
        "ds": pd.date_range(
            start=pd.Timestamp(forecast_date),
            periods=24,
            freq="h"
        )
    })

    weather = pd.DataFrame(list(weather_data))

    if weather.empty or "datetime" not in weather.columns:
        raise HttpError(404, "Weather data not available")

    weather = weather.rename(columns={"datetime": "ds"})
    weather["ds"] = pd.to_datetime(weather["ds"]).dt.tz_localize(None)

    future = future.merge(weather, on="ds", how="left")

    if future["temperature_c"].isna().any():
        raise HttpError(404, "Incomplete weather data for forecast date")

    # =====================================================
    # 4️⃣ Add calendar features
    # =====================================================
    future = add_calendar_features(future)

    # =====================================================
    # 5️⃣ Region hourly forecast (shape only)
    # =====================================================
    region_fc = region_model.predict(future)[["ds", "yhat"]]
    region_fc["temperature_c"] = future["temperature_c"]

    # =====================================================
    # 6️⃣ State daily forecast (ENERGY in MU/day)
    # =====================================================
    state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})
    state_future = add_calendar_features(state_future)

    state_fc = state_model.predict(state_future)[["ds", "yhat"]]

    energy_mu_per_day = float(state_fc["yhat"][0])

    # =====================================================
    # 7️⃣ MU → MW conversion (FINAL & CORRECT)
    # =====================================================
    average_load_mw = round((energy_mu_per_day * 1000) / 24, 2)

    # =====================================================
    # 8️⃣ Hourly MW disaggregation
    # =====================================================
    hourly = disaggregate(
        state_forecast=state_fc,
        region_forecast=region_fc,
        freq="H"
    )

    for p in hourly:
        if isinstance(p["datetime"], (pd.Timestamp, datetime)):
            p["datetime"] = p["datetime"].isoformat()

    # =====================================================
    # 9️⃣ Peak load (MW) — ONLY from hourly MW
    # =====================================================
    peak_load_mw = round(
        max(p["mw"] for p in hourly),
        2
    )

    # =====================================================
    # 🔟 Save daily prediction (MW)
    # =====================================================
    with transaction.atomic():
        DailyPredictionHistory.objects.update_or_create(
            state=state_short.name,
            date=forecast_date,
            defaults={
                "load_mw": average_load_mw
            }
        )

    # =====================================================
    # 1️⃣1️⃣ Final response (NO MAPE, NO NULL)
    # =====================================================
    return {
        "state": state_short,
        "date": forecast_date.isoformat(),
        **day_metadata(forecast_date),
        "energy_consumption_mu_per_day": round(energy_mu_per_day, 2),
        "average_load_mw": average_load_mw,
        "peak_load_mw": peak_load_mw,
        "points": hourly
    }


