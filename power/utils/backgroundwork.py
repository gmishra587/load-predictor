# from power.ml.manage_models import STATE_TO_REGION
# from power.ml.model_store import load_model, save_model
# from power.ml.weather import fetch_and_save_weather
# from power.ml.train import train_region_model, train_state_daily



# def background_work(state_short: str, start_date: str, frequency: str):
#     # Weather fetch & save
#     weather_data = fetch_and_save_weather(state_short, start_date, frequency=frequency)

#     # Load pre-trained models safely
#     region_code = STATE_TO_REGION[state_short]

#     try:
#         region_model = load_model(f"region_{region_code}.pkl")
#     except FileNotFoundError:
#         print(f"Region model {region_code} not found, training now...")
#         region_model = train_region_model(region_code)
#         save_model(f"region_{region_code}.pkl", region_model)

#     try:
#         state_model = load_model(f"state_{state_short}.pkl")
#     except FileNotFoundError:
#         print(f"State model {state_short} not found, training now...")
#         state_model = train_state_daily(state_short)
#         save_model(f"state_{state_short}.pkl", state_model)

#     return weather_data, region_model, state_model





from power.ml.manage_models import STATE_TO_REGION
from power.ml.model_store import load_model, save_model
from power.ml.weather import fetch_and_save_weather
from power.ml.train import (train_region_model, train_state_daily, train_state_5min_model)
from power.utils.logger import get_logger
# from ninja.errors import HttpError



logger = get_logger(__name__)





def background_work(state_short: str, start_date: str, frequency: str):
    logger.info("🚀 Background work started for state %s", state_short)

    # =========================
    # WEATHER
    # =========================
    try:
        logger.info("🌦️ Fetching weather data (%s) for %s", frequency, state_short)
        weather_data = fetch_and_save_weather(
            state_short,
            start_date=start_date,
            frequency=frequency,
        )
        logger.info("✅ Weather data ready for %s", state_short)
    except Exception as e:
        logger.error("❌ Weather fetch failed for %s: %s", state_short, e)
        # raise HttpError(status_code=500, message=f"Weather fetch failed: {e}")




    # =========================
    # REGION MODEL
    # =========================
    region_code = STATE_TO_REGION[state_short]

    try:
        region_model = load_model(f"region_{region_code}.pkl")
        logger.info("✅ Loaded region model [%s]", region_code)
    except FileNotFoundError:
        logger.warning("⚠️ Region model [%s] not found, training now", region_code)
        region_model = train_region_model(region_code)
        save_model(f"region_{region_code}.pkl", region_model)
        logger.info("💾 Region model saved [%s]", region_code)




    # =========================
    # STATE DAILY MODEL
    # =========================
    try:
        state_daily_model = load_model(f"state_daily_{state_short}.pkl")
        logger.info("✅ Loaded daily state model [%s]", state_short)
    except FileNotFoundError:
        logger.warning(
            "⚠️ Daily state model [%s] not found, training now", state_short
        )
        state_daily_model = train_state_daily(state_short)
        save_model(f"state_daily_{state_short}.pkl", state_daily_model)
        logger.info("💾 Daily state model saved [%s]", state_short)




    # =========================
    # STATE 5-MIN MODEL 🔥
    # =========================
    try:
        state_5min_model = load_model(f"state_5min_{state_short}.pkl")
        logger.info("✅ Loaded 5-minute state model [%s]", state_short)
    except FileNotFoundError:
        logger.warning(
            "⚠️ 5-minute state model [%s] not found, training now", state_short
        )
        state_5min_model = train_state_5min_model(state_short)
        save_model(f"state_5min_{state_short}.pkl", state_5min_model)
        logger.info("💾 5-minute state model saved [%s]", state_short)

    logger.info("🎯 Background work completed successfully for %s", state_short)

    return  weather_data, region_model, state_daily_model, state_5min_model
    
