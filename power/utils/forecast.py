# from datetime import datetime
# import pandas as pd
# from ninja.errors import HttpError
# from django.db import transaction

# from power.schemas import StateShortEnum
# from power.utils.backgroundwork import background_work
# from power.utils.metadata import add_calendar_features, day_metadata
# from power.utils.validation import validate_date
# from power.ml.disaggregate import disaggregate
# from power.models import DailyPredictionHistory


# def get_hourly_forecast_data(state_short: StateShortEnum, date: str):
#     # =====================================================
#     # 1️⃣ Validate date
#     # =====================================================
#     forecast_date = validate_date(date)

#     # =====================================================
#     # 2️⃣ Load weather + models
#     # =====================================================
#     weather_data, region_model, state_model = background_work(
#         state_short=state_short.value,
#         start_date=forecast_date,
#         frequency="hourly"
#     )

#     # =====================================================
#     # 3️⃣ Create future hourly dataframe
#     # =====================================================
#     future = pd.DataFrame({
#         "ds": pd.date_range(
#             start=pd.Timestamp(forecast_date),
#             periods=24,
#             freq="h"
#         )
#     })

#     weather = pd.DataFrame(list(weather_data))

#     if weather.empty or "datetime" not in weather.columns:
#         raise HttpError(404, "Weather data not available")

#     weather = weather.rename(columns={"datetime": "ds"})
#     weather["ds"] = pd.to_datetime(weather["ds"]).dt.tz_localize(None)

#     future = future.merge(weather, on="ds", how="left")

#     if future["temperature_c"].isna().any():
#         raise HttpError(404, "Incomplete weather data for forecast date")

#     # =====================================================
#     # 4️⃣ Add calendar features
#     # =====================================================
#     future = add_calendar_features(future)

#     # =====================================================
#     # 5️⃣ Region hourly forecast (shape only)
#     # =====================================================
#     region_fc = region_model.predict(future)[["ds", "yhat"]]
#     region_fc["temperature_c"] = future["temperature_c"]

#     # =====================================================
#     # 6️⃣ State daily forecast (ENERGY in MU/day)
#     # =====================================================
#     state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})
#     state_future = add_calendar_features(state_future)

#     state_fc = state_model.predict(state_future)[["ds", "yhat"]]

#     energy_mu_per_day = float(state_fc["yhat"][0])

#     # =====================================================
#     # 7️⃣ MU → MW conversion
#     # =====================================================
#     average_load_mw = round((energy_mu_per_day * 1000) / 24, 2)

#     # =====================================================
#     # 8️⃣ Hourly MW disaggregation
#     # =====================================================
#     hourly = disaggregate(
#         state_forecast=state_fc,
#         region_forecast=region_fc,
#         freq="H"
#     )

#     for p in hourly:
#         if isinstance(p["datetime"], (pd.Timestamp, datetime)):
#             p["datetime"] = p["datetime"].isoformat()

#     # =====================================================
#     # 🆕 8️⃣A Daily temperature summary (NEW)
#     # =====================================================
#     temps = [
#         p["temperature"]
#         for p in hourly
#         if p.get("temperature") is not None
#     ]

#     daily_temperature = None
#     if temps:
#         daily_temperature = {
#             "min": round(min(temps), 2),
#             "max": round(max(temps), 2),
#             "average": round(sum(temps) / len(temps), 2)
#         }

#     # =====================================================
#     # 9️⃣ Peak load (MW)
#     # =====================================================
#     peak_load_mw = round(
#         max(p["mw"] for p in hourly),
#         2
#     )

#     # =====================================================
#     # 🔟 Save daily prediction
#     # =====================================================
#     with transaction.atomic():
#         DailyPredictionHistory.objects.update_or_create(
#             state=state_short.value,
#             date=forecast_date,
#             defaults={
#                 "load_mw": average_load_mw
#             }
#         )

#     # =====================================================
#     # 1️⃣1️⃣ Final response
#     # =====================================================
#     return {
#         "state": state_short,
#         "date": forecast_date.isoformat(),
#         **day_metadata(forecast_date),
#         "energy_consumption_mu_per_day": round(energy_mu_per_day, 2),
#         "average_load_mw": average_load_mw,
#         "peak_load_mw": peak_load_mw,
#         "daily_temperature": daily_temperature,   # 👈 NEW
#         "points": hourly
#     }




# from datetime import datetime
# import pandas as pd
# from ninja.errors import HttpError
# from django.db import transaction
# from power.schemas import StateShortEnum
# from power.utils.backgroundwork import background_work
# from power.utils.metadata import add_calendar_features, day_metadata
# from power.utils.validation import validate_date
# from power.ml.disaggregate import disaggregate
# from power.models import DailyPredictionHistory
# from power.models import DailyPredictionHistory, StateLoad5Min



# def get_hourly_forecast_data(state_short: StateShortEnum, date: str):
#     # =====================================================
#     # 1️⃣ Validate date
#     # =====================================================
#     forecast_date = validate_date(date)

#     # =====================================================
#     # 2️⃣ Load weather + models
#     # =====================================================
#     weather_data, region_model, state_model, state_5min_model = background_work(
#         state_short=state_short.value,
#         start_date=forecast_date,
#         frequency="hourly"
#     )

#     # =====================================================
#     # 3️⃣ Create future hourly dataframe
#     # =====================================================
#     future = pd.DataFrame({
#         "ds": pd.date_range(
#             start=pd.Timestamp(forecast_date),
#             periods=24,
#             freq="h"
#         )
#     })

#     weather = pd.DataFrame(list(weather_data))

#     if weather.empty or "datetime" not in weather.columns:
#         raise HttpError(404, "Weather data not available")

#     weather = weather.rename(columns={"datetime": "ds"})
#     weather["ds"] = pd.to_datetime(weather["ds"]).dt.tz_localize(None)

#     future = future.merge(weather, on="ds", how="left")

#     if future["temperature_c"].isna().any():
#         raise HttpError(404, "Incomplete weather data for forecast date")

#     # =====================================================
#     # 4️⃣ Add calendar features
#     # =====================================================
#     future = add_calendar_features(future)

#     # =====================================================
#     # 5️⃣ Region hourly forecast (shape only)
#     # =====================================================
#     region_fc = region_model.predict(future)[["ds", "yhat"]]
#     region_fc["temperature_c"] = future["temperature_c"]

#     # =====================================================
#     # 6️⃣ State daily forecast (ENERGY in MU/day)
#     # =====================================================
#     # state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})
#     # state_future = add_calendar_features(state_future)

#     # state_fc = state_model.predict(state_future)[["ds", "yhat"]]
#     state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})

#     weather_df = pd.DataFrame(list(weather_data))
#     weather_df = weather_df.rename(columns={"datetime": "ds"})
#     weather_df["ds"] = pd.to_datetime(weather_df["ds"]).dt.tz_localize(None)

#     state_future = state_future.merge(weather_df, on="ds", how="left")

#     if state_future["temperature_c"].isna().any():
#         avg_temp = weather_df["temperature_c"].mean()
#         state_future["temperature_c"] = avg_temp

#     state_future = add_calendar_features(state_future)

#     state_fc = state_model.predict(state_future)[["ds", "yhat"]]


#     energy_mu_per_day = float(state_fc["yhat"][0])

#     # =====================================================
#     # 7️⃣ MU → MW conversion (FINAL & CORRECT)
#     # =====================================================
#     average_load_mw = round((energy_mu_per_day * 1000) / 24, 2)

#     # =====================================================
#     # 8️⃣ Hourly MW disaggregation
#     # =====================================================
#     hourly = disaggregate(
#         state_forecast=state_fc,
#         region_forecast=region_fc,
#         freq="H"
#     )

#     for p in hourly:
#         if isinstance(p["datetime"], (pd.Timestamp, datetime)):
#             p["datetime"] = p["datetime"].isoformat()


#     # =====================================================
#     # 🆕 8️⃣A Daily temperature summary (NEW)
#     # =====================================================
#     temps = [
#         p["temperature"]
#         for p in hourly
#         if p.get("temperature") is not None
#     ]

#     daily_temperature = None
#     if temps:
#         daily_temperature = {
#             "min": round(min(temps), 2),
#             "max": round(max(temps), 2),
#             "average": round(sum(temps) / len(temps), 2)
#         }

    

#     # =====================================================
#     # 9️⃣ Peak load (MW) — ONLY from hourly MW
#     # =====================================================
#     peak_load_mw = round(
#         max(p["mw"] for p in hourly),
#         2
#     )

#     # =====================================================
#     # 🔟 Save daily prediction (MW)
#     # =====================================================
#     with transaction.atomic():
#         DailyPredictionHistory.objects.update_or_create(
#             state=state_short.value,
#             date=forecast_date,
#             defaults={
#                 "load_mw": average_load_mw
#             }
#         )

#     # =====================================================
#     # 1️⃣1️⃣ Final response (NO MAPE, NO NULL)
#     # =====================================================
#     return {
#         "state": state_short,
#         "date": forecast_date.isoformat(),
#         **day_metadata(forecast_date),
#         "energy_consumption_mu_per_day": round(energy_mu_per_day, 2),
#         "average_load_mw": average_load_mw,
#         "peak_load_mw": peak_load_mw,
#         "daily_temperature": daily_temperature,   # 👈 NEW
#         "points": hourly
#     }





# def get_forecast_5min_data(state: StateShortEnum, forecast_date: datetime.date):
#     forecast_date = validate_date(forecast_date)

#     # 🔹 Background work (models + weather)
#     weather_data, region_model, state_model, state_5min_model = background_work(
#         state_short=state.value,
#         start_date=forecast_date,
#         frequency="hourly"
#     )

#     # 🔹 5-minute future timeline (1 day)
#     periods = 24 * 12  # 288
#     future = pd.DataFrame({
#         "ds": pd.date_range(
#             start=pd.Timestamp(forecast_date),
#             periods=periods,
#             freq="5min"
#         )
#     })

#     # 🔹 Weather → DataFrame
#     weather_df = pd.DataFrame(list(weather_data))
#     if weather_df.empty or "datetime" not in weather_df.columns:
#         raise ValueError("Weather data not available")

#     weather_df = weather_df.rename(columns={"datetime": "ds"})
#     weather_df["ds"] = pd.to_datetime(weather_df["ds"]).dt.tz_localize(None)

#     # 🔹 Sanity filter (realistic temperature)
#     weather_df = weather_df[weather_df["temperature_c"].between(-10, 55)]

#     # 🔹 Merge hourly weather → 5-min
#     future = future.merge(weather_df[["ds", "temperature_c"]], on="ds", how="left")

#     # 🔹 Fill missing temperature
#     future["temperature_c"] = future["temperature_c"].interpolate(limit_direction="both")

#     if future["temperature_c"].isna().any():
#         future["temperature_c"] = future["temperature_c"].fillna(
#             weather_df["temperature_c"].mean()
#         )

#     # 🔹 Calendar features
#     future = add_calendar_features(future)

#     # 🔹 Prediction
#     forecast_df = state_5min_model.predict(future)

#     # 🔹 Daily stats
#     average_load_mw = round(forecast_df["yhat"].mean(), 2)
#     peak_load_mw = round(forecast_df["yhat"].max(), 2)
#     energy_mu_per_day = round((average_load_mw * 24) / 1000, 2)


#     # =====================================================
#     # 🆕 Daily temperature summary (FROM 5-MIN DATA)
#     # =====================================================
#     temps = future["temperature_c"].dropna().tolist()

#     daily_temperature = None
#     if temps:
#         daily_temperature = {
#             "min": round(min(temps), 2),
#             "max": round(max(temps), 2),
#             "average": round(sum(temps) / len(temps), 2),
#         }

#     # 🔹 Save history
#     with transaction.atomic():
#         DailyPredictionHistory.objects.update_or_create(
#             state=state.value,
#             date=forecast_date,
#             defaults={"load_mw": average_load_mw}
#         )

#     # 🔹 Build response points (IMPORTANT FIX)
#     points = []
#     for i, row in forecast_df.iterrows():
#         points.append({
#             "datetime": row["ds"].isoformat(),
#             "mw": round(row["yhat"], 2),
#             "temperature": round(float(future.loc[i, "temperature_c"]), 1)
#         })

#     # 🔹 Final response
#     return {
#         "state": state,
#         "date": forecast_date.isoformat(),
#         **day_metadata(forecast_date),
#         "energy_consumption_mu_per_day": energy_mu_per_day,
#         "average_load_mw": average_load_mw,
#         "peak_load_mw": peak_load_mw,
#         "daily_temperature": daily_temperature,   # 👈 NEW
#         "points": points
#     }






# def get_forecast_5min_data(state: StateShortEnum, forecast_date: datetime.date):
#     forecast_date = validate_date(forecast_date)

#     # =====================================================
#     # Background work (models + weather)
#     # =====================================================
#     weather_data, region_model, state_model, state_5min_model = background_work(
#         state_short=state.value,
#         start_date=forecast_date,
#         frequency="hourly"
#     )

#     # =====================================================
#     # 5-minute timeline
#     # =====================================================
#     periods = 24 * 12  # 288
#     future = pd.DataFrame({
#         "ds": pd.date_range(
#             start=pd.Timestamp(forecast_date),
#             periods=periods,
#             freq="5min"
#         )
#     })

#     # =====================================================
#     # Weather
#     # =====================================================
#     weather_df = pd.DataFrame(list(weather_data))
#     if weather_df.empty or "datetime" not in weather_df.columns:
#         raise HttpError(404, "Weather data not available")

#     weather_df = weather_df.rename(columns={"datetime": "ds"})
#     weather_df["ds"] = pd.to_datetime(weather_df["ds"]).dt.tz_localize(None)

#     future = future.merge(
#         weather_df[["ds", "temperature_c"]],
#         on="ds",
#         how="left"
#     )

#     future["temperature_c"] = future["temperature_c"].interpolate(
#         limit_direction="both"
#     )

#     if future["temperature_c"].isna().any():
#         future["temperature_c"] = future["temperature_c"].fillna(
#             weather_df["temperature_c"].mean()
#         )

#     # =====================================================
#     # Calendar features
#     # =====================================================
#     future = add_calendar_features(future)

#     # =====================================================
#     # DAILY ENERGY (MU) — SOURCE OF TRUTH
#     # =====================================================
#     state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})
#     state_future = state_future.merge(weather_df, on="ds", how="left")

#     if state_future["temperature_c"].isna().any():
#         state_future["temperature_c"] = weather_df["temperature_c"].mean()

#     state_future = add_calendar_features(state_future)
#     state_fc = state_model.predict(state_future)

#     energy_mu_per_day = float(state_fc["yhat"].iloc[0])

#     # =====================================================
#     # ENERGY → AVERAGE MW
#     # =====================================================
#     average_load_mw = round((energy_mu_per_day * 1000) / 24, 2)

#     # =====================================================
#     # 🔥 DELHI SPECIAL FIX (VIDYUT PRAVAH MATCH)
#     # =====================================================
#     if state.value == "DL":

#         DELHI_PEAK_FACTOR = 1.35   # 🔑 calibrated

#         target_peak_mw = average_load_mw * DELHI_PEAK_FACTOR

#         # historical shape from your own DB (5-min)
#         hist = (
#             StateLoad5Min.objects
#             .filter(state="DL")
#             .values("datetime", "load_mw")
#         )

#         hist_df = pd.DataFrame(hist)
#         hist_df["minute"] = (
#             pd.to_datetime(hist_df["datetime"]).dt.hour * 60 +
#             pd.to_datetime(hist_df["datetime"]).dt.minute
#         )

#         shape = (
#             hist_df.groupby("minute")["load_mw"]
#             .mean()
#             .reset_index()
#         )

#         shape["fraction"] = shape["load_mw"] / shape["load_mw"].sum()

#         points = []
#         for _, row in future.iterrows():
#             minute = row["ds"].hour * 60 + row["ds"].minute
#             frac = shape.loc[
#                 shape["minute"] == minute,
#                 "fraction"
#             ].values[0]

#             mw = frac * target_peak_mw * 288

#             points.append({
#                 "datetime": row["ds"].isoformat(),
#                 "mw": round(mw, 2),
#                 "temperature": round(float(row["temperature_c"]), 1)
#             })

#         peak_load_mw = round(max(p["mw"] for p in points), 2)

#     # =====================================================
#     # NON-DELHI → OLD MODEL
#     # =====================================================
#     else:
#         forecast_df = state_5min_model.predict(future)

#         points = [
#             {
#                 "datetime": row.ds.isoformat(),
#                 "mw": round(row.yhat, 2),
#                 "temperature": round(float(future.loc[i, "temperature_c"]), 1)
#             }
#             for i, row in forecast_df.iterrows()
#         ]

#         peak_load_mw = round(max(p["mw"] for p in points), 2)

#     # =====================================================
#     # DAILY TEMPERATURE
#     # =====================================================
#     temps = future["temperature_c"].tolist()
#     daily_temperature = {
#         "min": round(min(temps), 2),
#         "max": round(max(temps), 2),
#         "average": round(sum(temps) / len(temps), 2)
#     }

#     # =====================================================
#     # SAVE HISTORY
#     # =====================================================
#     with transaction.atomic():
#         DailyPredictionHistory.objects.update_or_create(
#             state=state.value,
#             date=forecast_date,
#             defaults={"load_mw": average_load_mw}
#         )

#     # =====================================================
#     # FINAL RESPONSE
#     # =====================================================
#     return {
#         "state": state.value,
#         "date": forecast_date.isoformat(),
#         **day_metadata(forecast_date),
#         "energy_consumption_mu_per_day": round(energy_mu_per_day, 2),
#         "average_load_mw": average_load_mw,
#         "peak_load_mw": peak_load_mw,
#         "daily_temperature": daily_temperature,
#         "points": points
#     }








# from datetime import datetime, date
# import pandas as pd

# from ninja.errors import HttpError
# from django.db import transaction

# from power.schemas import StateShortEnum
# from power.utils.backgroundwork import background_work
# from power.utils.metadata import add_calendar_features, day_metadata
# from power.utils.validation import validate_date
# from power.ml.disaggregate import disaggregate
# from power.models import DailyPredictionHistory


# # =====================================================
# # HOURLY FORECAST (ENERGY ANCHORED)
# # =====================================================
# def get_hourly_forecast_data(state_short: StateShortEnum, date: str):
#     forecast_date = validate_date(date)

#     weather_data, region_model, state_model, _ = background_work(
#         state_short=state_short.value,
#         start_date=forecast_date,
#         frequency="hourly"
#     )

#     # ---- Future hourly timeline
#     future = pd.DataFrame({
#         "ds": pd.date_range(
#             start=pd.Timestamp(forecast_date),
#             periods=24,
#             freq="h"
#         )
#     })

#     weather_df = pd.DataFrame(list(weather_data))
#     if weather_df.empty or "datetime" not in weather_df.columns:
#         raise HttpError(404, "Weather data not available")

#     weather_df = weather_df.rename(columns={"datetime": "ds"})
#     weather_df["ds"] = pd.to_datetime(weather_df["ds"]).dt.tz_localize(None)

#     future = future.merge(weather_df, on="ds", how="left")
#     future["temperature_c"] = future["temperature_c"].interpolate(limit_direction="both")

#     future = add_calendar_features(future)

#     # ---- Region shape
#     region_fc = region_model.predict(future)[["ds", "yhat"]]
#     region_fc["temperature_c"] = future["temperature_c"]

#     # ---- State daily energy (MU/day)
#     state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})
#     state_future = state_future.merge(weather_df, on="ds", how="left")
#     state_future["temperature_c"] = state_future["temperature_c"].fillna(
#         weather_df["temperature_c"].mean()
#     )
#     state_future = add_calendar_features(state_future)

#     state_fc = state_model.predict(state_future)[["ds", "yhat"]]
#     energy_mu_per_day = float(state_fc["yhat"][0])

#     # ---- Convert MU → MW(avg)
#     average_load_mw = round((energy_mu_per_day * 1000) / 24, 2)

#     # ---- Hourly disaggregation
#     hourly = disaggregate(
#         state_forecast=state_fc,
#         region_forecast=region_fc,
#         freq="H"
#     )

#     for p in hourly:
#         if isinstance(p["datetime"], (pd.Timestamp, datetime)):
#             p["datetime"] = p["datetime"].isoformat()

#     peak_load_mw = round(max(p["mw"] for p in hourly), 2)

#     # ---- Save
#     with transaction.atomic():
#         DailyPredictionHistory.objects.update_or_create(
#             state=state_short.value,
#             date=forecast_date,
#             defaults={"load_mw": average_load_mw}
#         )

#     temps = [p["temperature"] for p in hourly if p.get("temperature") is not None]
#     daily_temperature = None
#     if temps:
#         daily_temperature = {
#             "min": round(min(temps), 2),
#             "max": round(max(temps), 2),
#             "average": round(sum(temps) / len(temps), 2),
#         }

#     return {
#         "state": state_short,
#         "date": forecast_date.isoformat(),
#         **day_metadata(forecast_date),
#         "energy_consumption_mu_per_day": round(energy_mu_per_day, 2),
#         "average_load_mw": average_load_mw,
#         "peak_load_mw": peak_load_mw,
#         "daily_temperature": daily_temperature,
#         "points": hourly
#     }


# # =====================================================
# # 5-MIN FORECAST (SLDC MATCHED, ENERGY LOCKED)
# # =====================================================
# def get_forecast_5min_data(state: StateShortEnum, forecast_date: date):
#     forecast_date = validate_date(forecast_date)

#     weather_data, region_model, state_model, state_5min_model = background_work(
#         state_short=state.value,
#         start_date=forecast_date,
#         frequency="hourly"
#     )

#     # -------------------------------------------------
#     # DAILY ENERGY (SINGLE SOURCE OF TRUTH)
#     # -------------------------------------------------
#     state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})

#     weather_df = pd.DataFrame(list(weather_data))
#     if weather_df.empty or "datetime" not in weather_df.columns:
#         raise HttpError(404, "Weather data not available")

#     weather_df = weather_df.rename(columns={"datetime": "ds"})
#     weather_df["ds"] = pd.to_datetime(weather_df["ds"]).dt.tz_localize(None)

#     state_future = state_future.merge(weather_df, on="ds", how="left")
#     state_future["temperature_c"] = state_future["temperature_c"].fillna(
#         weather_df["temperature_c"].mean()
#     )
#     state_future = add_calendar_features(state_future)

#     state_fc = state_model.predict(state_future)[["ds", "yhat"]]
#     energy_mu_per_day = float(state_fc["yhat"][0])
#     target_energy_mwh = energy_mu_per_day * 1000

#     # -------------------------------------------------
#     # 5-MIN TIMELINE
#     # -------------------------------------------------
#     future = pd.DataFrame({
#         "ds": pd.date_range(
#             start=pd.Timestamp(forecast_date),
#             periods=288,
#             freq="5min"
#         )
#     })

#     future = future.merge(weather_df[["ds", "temperature_c"]], on="ds", how="left")
#     future["temperature_c"] = future["temperature_c"].interpolate(limit_direction="both")
#     future["temperature_c"] = future["temperature_c"].fillna(
#         weather_df["temperature_c"].mean()
#     )

#     future = add_calendar_features(future)

#     # -------------------------------------------------
#     # RAW 5-MIN PREDICTION (SHAPE ONLY)
#     # -------------------------------------------------
#     forecast_df = state_5min_model.predict(future)

#     # -------------------------------------------------
#     # 🔥 ENERGY NORMALIZATION (MOST IMPORTANT)
#     # -------------------------------------------------
#     interval_hours = 5 / 60  # 0.08333
#     predicted_energy_mwh = forecast_df["yhat"].sum() * interval_hours
#     scale_factor = target_energy_mwh / predicted_energy_mwh
#     forecast_df["yhat"] = forecast_df["yhat"] * scale_factor

#     # -------------------------------------------------
#     # DELHI NIGHT CORRECTION (SLDC BEHAVIOUR)
#     # -------------------------------------------------
#     if state.value == "DL":
#         for i, row in forecast_df.iterrows():
#             hour = row["ds"].hour
#             if 0 <= hour < 5:
#                 forecast_df.at[i, "yhat"] *= 1.12
#             if 9 <= hour <= 11:
#                 forecast_df.at[i, "yhat"] *= 1.05

#     # -------------------------------------------------
#     # FINAL STATS
#     # -------------------------------------------------
#     average_load_mw = round(forecast_df["yhat"].mean(), 2)
#     peak_load_mw = round(forecast_df["yhat"].max(), 2)

#     temps = future["temperature_c"].tolist()
#     daily_temperature = {
#         "min": round(min(temps), 2),
#         "max": round(max(temps), 2),
#         "average": round(sum(temps) / len(temps), 2),
#     }

#     with transaction.atomic():
#         DailyPredictionHistory.objects.update_or_create(
#             state=state.value,
#             date=forecast_date,
#             defaults={"load_mw": average_load_mw}
#         )

#     # -------------------------------------------------
#     # RESPONSE POINTS
#     # -------------------------------------------------
#     points = []
#     for i, row in forecast_df.iterrows():
#         points.append({
#             "datetime": row["ds"].isoformat(),
#             "mw": round(row["yhat"], 2),
#             "temperature": round(float(future.loc[i, "temperature_c"]), 1)
#         })

#     return {
#         "state": state,
#         "date": forecast_date.isoformat(),
#         **day_metadata(forecast_date),
#         "energy_consumption_mu_per_day": round(energy_mu_per_day, 2),
#         "average_load_mw": average_load_mw,
#         "peak_load_mw": peak_load_mw,
#         "daily_temperature": daily_temperature,
#         "points": points
#     }



from datetime import date
import pandas as pd
import numpy as np

from django.db import transaction
from ninja.errors import HttpError

from power.schemas import StateShortEnum
from power.utils.validation import validate_date
from power.utils.backgroundwork import background_work
from power.utils.metadata import add_calendar_features, day_metadata
from power.ml.disaggregate import disaggregate
from power.models import DailyPredictionHistory


# =====================================================
# CONSTANTS (NO STATIC LOAD VALUES)
# =====================================================
MAX_RAMP_MW_5MIN = 110


# =====================================================
# RAMP LIMITER
# =====================================================
def apply_ramp_limit(series: pd.Series, max_step: float) -> pd.Series:
    values = series.values.copy()
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        if diff > max_step:
            values[i] = values[i - 1] + max_step
        elif diff < -max_step:
            values[i] = values[i - 1] - max_step
    return pd.Series(values, index=series.index)


# =====================================================
# HOURLY FORECAST
# =====================================================
def get_hourly_forecast_data(state_short: StateShortEnum, date: str):
    forecast_date = validate_date(date)

    weather_data, region_model, state_model, _ = background_work(
        state_short=state_short.value,
        start_date=forecast_date,
        frequency="hourly",
    )

    weather_df = pd.DataFrame(list(weather_data))
    if weather_df.empty:
        raise HttpError(404, "Weather data not available")

    weather_df["ds"] = pd.to_datetime(weather_df["datetime"]).dt.tz_localize(None)

    future = pd.DataFrame({
        "ds": pd.date_range(start=pd.Timestamp(forecast_date), periods=24, freq="h")
    })

    future = future.merge(
        weather_df[["ds", "temperature_c"]],
        on="ds",
        how="left",
    )

    future["temperature_c"] = future["temperature_c"].interpolate(
        limit_direction="both"
    )

    future = add_calendar_features(future)

    region_fc = region_model.predict(future)[["ds", "yhat"]]
    region_fc["temperature"] = future["temperature_c"]

    # DAILY ENERGY
    state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})
    state_future["temperature_c"] = float(
        weather_df["temperature_c"].mean()
    )
    state_future = add_calendar_features(state_future)

    energy_mu = float(state_model.predict(state_future)["yhat"].iloc[0])
    average_load = round((energy_mu * 1000) / 24, 2)

    hourly = disaggregate(
        state_forecast=state_future.assign(yhat=energy_mu),
        region_forecast=region_fc,
        freq="H",
    )

    peak_load = round(max(p["mw"] for p in hourly), 2)

    with transaction.atomic():
        DailyPredictionHistory.objects.update_or_create(
            state=state_short.value,
            date=forecast_date,
            defaults={"load_mw": average_load},
        )

    temps = [p.get("temperature", 0.0) for p in hourly]
    daily_temperature = {
        "min": round(min(temps), 2),
        "max": round(max(temps), 2),
        "average": round(sum(temps) / len(temps), 2),
    }

    return {
        "state": state_short,
        "date": forecast_date.isoformat(),
        **day_metadata(forecast_date),
        "energy_consumption_mu_per_day": round(energy_mu, 2),
        "average_load_mw": average_load,
        "peak_load_mw": peak_load,
        "daily_temperature": daily_temperature,
        "points": hourly,
    }


# =====================================================
# 5-MIN FORECAST
# =====================================================
def get_forecast_5min_data(state: StateShortEnum, forecast_date: date):
    forecast_date = validate_date(forecast_date)

    weather_data, _, state_model, state_5min_model = background_work(
        state_short=state.value,
        start_date=forecast_date,
        frequency="hourly",
    )

    weather_df = pd.DataFrame(list(weather_data))
    if weather_df.empty:
        raise HttpError(404, "Weather data not available")

    weather_df["ds"] = pd.to_datetime(weather_df["datetime"]).dt.tz_localize(None)

    # DAILY ENERGY
    state_future = pd.DataFrame({"ds": [pd.Timestamp(forecast_date)]})
    state_future["temperature_c"] = float(
        weather_df["temperature_c"].mean()
    )
    state_future = add_calendar_features(state_future)

    energy_mu = float(state_model.predict(state_future)["yhat"].iloc[0])
    target_energy_mwh = energy_mu * 1000

    # 5-MIN GRID
    future = pd.DataFrame({
        "ds": pd.date_range(
            start=pd.Timestamp(forecast_date),
            periods=288,
            freq="5min",
        )
    })

    future = future.merge(
        weather_df[["ds", "temperature_c"]],
        on="ds",
        how="left",
    )

    future["temperature_c"] = future["temperature_c"].interpolate(
        limit_direction="both"
    )

    future = add_calendar_features(future)

    forecast_df = state_5min_model.predict(future)[["ds", "yhat"]]

    interval_hours = 5 / 60

    # ENERGY NORMALIZATION
    forecast_df["yhat"] *= (
        target_energy_mwh /
        (forecast_df["yhat"].sum() * interval_hours)
    )

    # RAMP LIMIT
    forecast_df["yhat"] = apply_ramp_limit(
        forecast_df["yhat"], MAX_RAMP_MW_5MIN
    )

    # FINAL ENERGY LOCK
    forecast_df["yhat"] *= (
        target_energy_mwh /
        (forecast_df["yhat"].sum() * interval_hours)
    )

    average_load = round(forecast_df["yhat"].mean(), 2)
    peak_load = round(forecast_df["yhat"].max(), 2)

    with transaction.atomic():
        DailyPredictionHistory.objects.update_or_create(
            state=state.value,
            date=forecast_date,
            defaults={"load_mw": average_load},
        )

    points = [
        {
            "datetime": row.ds.isoformat(),
            "mw": round(row.yhat, 2),
            "temperature": round(
                float(future.loc[i, "temperature_c"]), 1
            ),
        }
        for i, row in forecast_df.iterrows()
    ]

    meta = day_metadata(forecast_date)

    return {
        "state": state,
        "date": forecast_date.isoformat(),
        **meta,
        "energy_consumption_mu_per_day": round(energy_mu, 2),
        "average_load_mw": average_load,
        "peak_load_mw": peak_load,
        "mape_difference_percent": None,
        "daily_temperature": {
            "min": round(future["temperature_c"].min(), 2),
            "max": round(future["temperature_c"].max(), 2),
            "average": round(future["temperature_c"].mean(), 2),
        },
        "points": points,
    }

























