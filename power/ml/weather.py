from datetime import datetime
import requests
import pandas as pd
from power.models import WeatherHourly
from django.db import transaction



STATE_COORDS = {
    "DL": {"lat": 28.6139, "lon": 77.2090},
    "MH": {"lat": 19.7515, "lon": 75.7139},
    "TN": {"lat": 11.1271, "lon": 78.6569},
    "UP": {"lat": 26.8467, "lon": 80.9462},
    "AP": {"lat": 15.9129, "lon": 79.7400},
    "AR": {"lat": 28.2180, "lon": 94.7278},
    "AS": {"lat": 26.2006, "lon": 92.9376},
    "BR": {"lat": 25.0961, "lon": 85.3131},
    "CH": {"lat": 30.7333, "lon": 76.7794},
    "CG": {"lat": 21.2787, "lon": 81.8661},
    "GA": {"lat": 15.2993, "lon": 74.1240},
    "GJ": {"lat": 22.2587, "lon": 71.1924},
    "HR": {"lat": 29.0588, "lon": 76.0856},
    "HP": {"lat": 31.1048, "lon": 77.1734},
    "JK": {"lat": 33.7782, "lon": 76.5762},
    "JH": {"lat": 23.6102, "lon": 85.2799},
    "KA": {"lat": 15.3173, "lon": 75.7139},
    "KL": {"lat": 10.8505, "lon": 76.2711},
    "MN": {"lat": 24.6637, "lon": 93.9063},
    "ML": {"lat": 25.4670, "lon": 91.3662},
    "MZ": {"lat": 23.1645, "lon": 92.9376},
    "MP": {"lat": 22.9734, "lon": 78.6569},
    "NL": {"lat": 26.1584, "lon": 94.5624},
    "OD": {"lat": 20.9517, "lon": 85.0985},
    "PY": {"lat": 11.9416, "lon": 79.8083},
    "PB": {"lat": 31.1471, "lon": 75.3412},
    "RJ": {"lat": 27.0238, "lon": 74.2179},
    "SK": {"lat": 27.5330, "lon": 88.5122},
    "TS": {"lat": 18.1124, "lon": 79.0193},
    "TR": {"lat": 23.9408, "lon": 91.9882},
    "UK": {"lat": 30.0668, "lon": 79.0193},
    "WB": {"lat": 22.9868, "lon": 87.8550},
}





def fetch_weather(state_short, start_date, frequency="hourly"):
    if state_short not in STATE_COORDS:
        raise ValueError(f"Coordinates not found for state: {state_short}")

    coords = STATE_COORDS[state_short]

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": round(coords["lat"], 4),  # precision fix
        "longitude": round(coords["lon"], 4),
        "hourly": "temperature_2m",
        "start_date": start_date,
        "end_date": start_date,
        "timezone": "Asia/Kolkata"
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.HTTPError as e:
        # Properly log HTTP errors
        print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        return pd.DataFrame(columns=["datetime", "temperature_c"])  # return empty df
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        return pd.DataFrame(columns=["datetime", "temperature_c"])

    # check if data exists
    if "hourly" not in data or "time" not in data["hourly"]:
        print("Weather API returned no data")
        return pd.DataFrame(columns=["datetime", "temperature_c"])

    df = pd.DataFrame({
        "datetime": data["hourly"]["time"],
        "temperature_c": data["hourly"]["temperature_2m"]
    })

    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)

    if frequency == "15min":
        df = df.set_index("datetime").resample("15T").interpolate().reset_index()

    return df





def save_weather(state_short: str, df: pd.DataFrame):
    # --- sanitize dataframe ---
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df = df.dropna(subset=["temperature_c"])

    if df.empty:
        return []

    datetimes = list(df["datetime"])

    existing = {
        w.datetime: w
        for w in WeatherHourly.objects.filter(
            state=state_short,
            datetime__in=datetimes
        )
    }

    to_create = []
    to_update = []

    with transaction.atomic():
        for _, row in df.iterrows():
            dt = row["datetime"]
            temp = float(row["temperature_c"])

            if dt in existing:
                obj = existing[dt]
                obj.temperature_c = temp
                to_update.append(obj)
            else:
                to_create.append(
                    WeatherHourly(
                        state=state_short,
                        datetime=dt,
                        temperature_c=temp
                    )
                )

        if to_create:
            WeatherHourly.objects.bulk_create(to_create)

        if to_update:
            WeatherHourly.objects.bulk_update(to_update, ["temperature_c"])

    saved = to_create + to_update

    print(f"Saved {len(saved)} weather records for {state_short}")

    # ✅ RETURN SERIALIZABLE DATA
    return [
        {
            "state": obj.state,
            "datetime": obj.datetime,
            "temperature_c": obj.temperature_c,
        }
        for obj in saved
    ]





def fetch_and_save_weather(state_short, start_date, frequency="hourly"):
    df = fetch_weather(state_short, start_date, frequency)
    # print(df)
    # return
    rows_saved = save_weather(state_short, df)
    return rows_saved