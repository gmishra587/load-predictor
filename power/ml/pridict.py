import pandas as pd
from power.ml.train import train_region_model, train_state_daily
from power.ml.weather import fetch_and_save_weather
from power.models import RegionHourlyLoad, StateDailyLoad, WeatherHourly
from datetime import timedelta
from power.utils.metadata import add_calendar_features






# ---------- Prediction Function ----------
def predict_region(region: str, periods: int = 24, freq: str = "H"):
    # Train model
    model = train_region_model(region)

    # Future dataframe
    last_date = RegionHourlyLoad.objects.filter(region=region).order_by("datetime").last().datetime
    future = pd.date_range(start=last_date + timedelta(minutes=1), periods=periods, freq=freq)
    future_df = pd.DataFrame({"ds": future})

    # Fetch weather for future dates
    for state_short in [region]:  # region as state_short for weather fetch
        for single_date in sorted(future_df["ds"].dt.date.unique()):
            fetch_and_save_weather(state_short, str(single_date), frequency="hourly" if freq=="H" else "15min")

    # Merge weather
    weather = pd.DataFrame(WeatherHourly.objects.filter(state=region).values("datetime", "temperature_c"))
    weather.rename(columns={"datetime": "ds"}, inplace=True)
    future_df = future_df.merge(weather, on="ds", how="left")

    # Add calendar features
    future_df = add_calendar_features(future_df)

    forecast = model.predict(future_df)
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]







def predict_state(state: str, periods: int = 7, freq: str = "D"):
    model = train_state_daily(state)

    last_date = StateDailyLoad.objects.filter(state=state).order_by("date").last().date
    future = pd.date_range(start=last_date + timedelta(days=1), periods=periods, freq=freq)
    future_df = pd.DataFrame({"ds": future})

    future_df = add_calendar_features(future_df)

    forecast = model.predict(future_df)
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
