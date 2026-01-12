import pandas as pd
from prophet import Prophet
from power.models import RegionHourlyLoad, StateDailyLoad, WeatherHourly
from power.utils.metadata import add_calendar_features
from django.db.models import Avg



REGION_TO_STATES = {
    "NR": ["DL", "UP", "HR", "PB", "HP", "JK", "UK", "CH"],
    "WR": ["MH", "GJ", "MP", "RJ", "CG", "GA"],
    "SR": ["TN", "AP", "TS", "KA", "KL", "PY"],
    "ER": ["WB", "BR", "OD", "JH"],
    "NER": ["AR", "AS", "MN", "ML", "MZ", "NL", "SK", "TR"],
}





# ---------- Region Model Training ----------
def train_region_model(region: str):
    df = pd.DataFrame(
        RegionHourlyLoad.objects.filter(region=region).values()
    )

    if df.empty:
        raise ValueError(f"No RegionHourlyLoad data for region {region}")

    df.rename(columns={"datetime": "ds", "load_mw": "y"}, inplace=True)
    df["ds"] = pd.to_datetime(df["ds"]).dt.tz_localize(None)

    states = REGION_TO_STATES.get(region)
    if not states:
        raise ValueError(f"No states mapped for region {region}")

    weather = pd.DataFrame(
        WeatherHourly.objects
        .filter(state__in=states)
        .values("datetime", "temperature_c")
    )

    if not weather.empty:
        weather.rename(columns={"datetime": "ds"}, inplace=True)
        weather["ds"] = pd.to_datetime(weather["ds"]).dt.tz_localize(None)

        weather = (
            weather.groupby("ds", as_index=False)["temperature_c"]
            .mean()
        )
    else:
        weather = pd.DataFrame({"ds": df["ds"], "temperature_c": None})

    df = df.merge(weather, on="ds", how="left")

    # 🔥 FINAL SAFETY NET
    if df["temperature_c"].isna().all():
        avg_temp = WeatherHourly.objects.filter(
            state__in=states
        ).aggregate(avg=Avg("temperature_c"))["avg"]

        if avg_temp is None:
            raise ValueError("No historical temperature data available")

        df["temperature_c"] = avg_temp
    else:
        df["temperature_c"] = df["temperature_c"].ffill().bfill()

    df = add_calendar_features(df)

    m = Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=True
    )

    m.add_regressor("temperature_c")
    m.add_regressor("is_weekend")
    m.add_regressor("is_holiday")
    m.add_regressor("season")

    m.fit(df)
    return m




# ---------- State Daily Model Training ----------
def train_state_daily(state: str):
    df = pd.DataFrame(
        StateDailyLoad.objects.filter(state=state).values()
    )
    if df.empty:
        raise ValueError(f"No StateDailyLoad data found for state: {state}")

    df.rename(columns={"date": "ds", "energy_mu": "y"}, inplace=True)
    df["ds"] = pd.to_datetime(df["ds"]).dt.tz_localize(None)

    df = add_calendar_features(df)

    m = Prophet(
        weekly_seasonality=True,
        yearly_seasonality=True
    )
    m.add_regressor("is_weekend")
    m.add_regressor("is_holiday")
    m.add_regressor("season")

    m.fit(df)
    return m
